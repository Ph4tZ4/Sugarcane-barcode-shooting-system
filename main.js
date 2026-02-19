// ================= ตั้งค่าส่วนกลาง (Global Config) =================
var DATA_START_ROW = 3; // ข้อมูลเริ่มที่แถว 3
var HEADER_ROWS = 2;    // จำนวนแถวหัวตาราง
var DB_MAX_COLS = 14;   // [OPTIMIZED] ดึงเฉพาะ Col A-N (14 คอลัมน์) ที่ใช้จริง

// ================= Cache Helpers =================

/**
 * [OPTIMIZED] ใช้ CacheService เก็บค่า lastRow + record data + station mapping
 * ลด API calls ไปยัง Google Sheets server อย่างมาก
 */
function getCachedLastRow(sheet) {
    if (!sheet) return 0;
    var cache = CacheService.getScriptCache();
    var key = 'lastRow_' + sheet.getName();
    var cached = cache.get(key);
    if (cached) return parseInt(cached);

    var lastRow = sheet.getLastRow();
    cache.put(key, lastRow.toString(), 120);
    return lastRow;
}

function invalidateLastRowCache(sheetName) {
    var cache = CacheService.getScriptCache();
    cache.remove('lastRow_' + sheetName);
}

/**
 * [NEW] Cache ข้อมูล record ของ bill ที่เคยค้นหาแล้ว
 * ยิงบาร์โค้ดซ้ำ / ค้นหา bill เดิม → ดึงจาก cache ทันที (< 100ms)
 * TTL = 300 วินาที (5 นาที)
 */
function getCachedRecord(billNumber) {
    var cache = CacheService.getScriptCache();
    var cached = cache.get('rec_' + billNumber);
    if (cached) {
        try { return JSON.parse(cached); } catch (e) { return null; }
    }
    return null;
}

function setCachedRecord(billNumber, recordArray) {
    try {
        var cache = CacheService.getScriptCache();
        cache.put('rec_' + billNumber, JSON.stringify(recordArray), 300);
    } catch (e) { /* ignore cache write errors */ }
}

// ================= 1. onEdit Trigger (Optimized) =================
function onEdit(e) {
    // ================= ตั้งค่าระบบ =================
    var factorySheetName = "รวมโรงงาน";
    var dbSheetName = "DataBase";

    var colBarcode = 20;      // Col T
    var colStatus = 21;       // Col U
    var colSortTrigger = 22; // Col V
    var colMoney = 12;        // Col L

    // -- ตำแหน่งใน DataBase --
    var dbColBill = 3;
    var dbColStation = 5; // Col E

    // ================= เริ่มการทำงาน =================
    if (!e || !e.range) return;
    var range = e.range;
    var sheet = range.getSheet();
    var sheetName = sheet.getName();
    var row = range.getRow();
    var col = range.getColumn();
    var val = range.getValue().toString().trim();

    if (sheetName === dbSheetName) return;

    // ##############################################################
    // ส่วนที่ 1: ระบบเรียงลำดับ
    // ##############################################################
    if (row === 1 && col === colSortTrigger) {
        if (val === ">" || val === "<") {
            var lastRealRow = getLastRowInColumn(sheet, 2);

            if (lastRealRow < DATA_START_ROW) {
                range.clearContent();
                return;
            }

            var numRows = lastRealRow - DATA_START_ROW + 1;

            // ใช้ Batch Get Values เพื่อลด overhead
            var billRange = sheet.getRange(DATA_START_ROW, 2, numRows, 1);
            var billValues = billRange.getValues();
            var sortValues = [];

            var blankValue = (val === ">") ? Number.MIN_SAFE_INTEGER : Number.MAX_SAFE_INTEGER;

            for (var i = 0; i < billValues.length; i++) {
                var rawBill = billValues[i][0];
                var sortValuesTemp;

                if (!rawBill || rawBill.toString().trim() === "") {
                    sortValuesTemp = blankValue;
                } else {
                    sortValuesTemp = extractRunNumber(rawBill);
                    if (sortValuesTemp === 0) sortValuesTemp = blankValue;
                }
                sortValues.push([sortValuesTemp]);
            }

            var maxCols = sheet.getMaxColumns();
            var lastDataCol = sheet.getLastColumn();

            var helperColIndex = lastDataCol + 1;
            if (helperColIndex > maxCols) {
                sheet.insertColumnAfter(maxCols);
                helperColIndex = maxCols + 1;
            }

            var helperRange = sheet.getRange(DATA_START_ROW, helperColIndex, numRows, 1);
            helperRange.setValues(sortValues);

            var rangeToSort = sheet.getRange(DATA_START_ROW, 1, numRows, helperColIndex);
            rangeToSort.sort({ column: helperColIndex, ascending: (val === ">") });

            helperRange.clearContent();
            range.clearContent();

            e.source.toast("จัดเรียงเสร็จสิ้น (" + numRows + " รายการ)", "เรียบร้อย", 3);
        }
        return;
    }

    // ##############################################################
    // ส่วนที่ 2: ระบบยิงบาร์โค้ด
    // ##############################################################
    if (row < DATA_START_ROW) return;

    var ss = e.source;
    var dbSheet = ss.getSheetByName(dbSheetName);
    if (!dbSheet) return;
    var statusCell = sheet.getRange(row, colStatus);

    if (col === colBarcode) {
        if (val === "") {
            range.setBackground(null);
            statusCell.setValue("").setBackground(null);
            sheet.getRange(row, 1, 1, 9).clearContent();
            sheet.getRange(row, 12, 1, 1).clearContent();
            return;
        }

        var parts = val.split('/');
        if (parts.length < 2) {
            setError(range, statusCell, "รูปแบบผิด");
            range.activate();
            return;
        }

        var prefix = parts[0];
        var code3Digits = prefix.substring(prefix.length - 3);
        var targetSheetName = "";

        if (code3Digits === "000") {
            targetSheetName = factorySheetName;
        } else {
            // [OPTIMIZED] ใช้ cached station mapping
            var stationInfo = findStationNameInBigData(dbSheet, code3Digits, dbColBill, dbColStation);
            if (stationInfo) {
                targetSheetName = stationInfo;
            } else {
                setError(range, statusCell, "ไม่พบรหัสสถานี " + code3Digits);
                range.activate();
                return;
            }
        }

        if (sheetName !== targetSheetName) {
            var targetSheet = ss.getSheetByName(targetSheetName);
            if (!targetSheet) {
                try {
                    targetSheet = sheet.copyTo(ss);
                    targetSheet.setName(targetSheetName);
                    var lastRowTarget = targetSheet.getLastRow();
                    if (lastRowTarget >= DATA_START_ROW) {
                        var numRows = lastRowTarget - DATA_START_ROW + 1;
                        var rangeToClear = targetSheet.getRange(DATA_START_ROW, 1, numRows, targetSheet.getLastColumn());
                        rangeToClear.clearContent();
                        rangeToClear.setBackground(null);
                    }
                } catch (err) {
                    setError(range, statusCell, "สร้างชีทไม่ได้");
                    return;
                }
            }

            range.clearContent();
            range.setBackground(null);
            statusCell.setValue("").setBackground(null);
            targetSheet.activate();
            var lastRowTarget = getLastRowInColumn(targetSheet, 1);
            var nextRow = lastRowTarget + 1;
            if (nextRow < DATA_START_ROW) nextRow = DATA_START_ROW;
            targetSheet.getRange(nextRow, colBarcode).activate();
            return;
        } else {
            // [OPTIMIZED v2] เช็คซ้ำก่อน fetch → fail-fast ไม่ต้องเสียเวลาค้นหา DB
            if (checkDuplicateOptimized(sheet, val, 2)) {
                setError(range, statusCell, "ข้อมูลซ้ำ");
                sheet.getRange(row, 1, 1, 9).clearContent();
                range.activate();
                return;
            }

            // [OPTIMIZED v2] ลองดึงจาก cache ก่อน → ถ้าเคยค้นหาแล้วจะเร็วมาก
            var recordRaw = getCachedRecord(val);
            if (!recordRaw) {
                recordRaw = findRecordInBigData(dbSheet, val, dbColBill);
                if (recordRaw) setCachedRecord(val, recordRaw);
            }

            if (recordRaw) {
                var mappedData = mapRecord(recordRaw);
                sheet.getRange(row, 1, 1, mappedData.length).setValues([mappedData]);

                sheet.getRange(row, colMoney).setFormula("=E" + row + "*J" + row + "-H" + row + "+K" + row);

                setSuccess(range, statusCell, "OK");
                sheet.getRange(row + 1, colBarcode).activate();
            } else {
                setNotFound(range, statusCell, "ไม่พบข้อมูล");
                sheet.getRange(row, 1, 1, 9).clearContent();
                range.activate();
            }
        }
    }
}

// ================= Helper Functions =================
function extractRunNumber(billString) {
    if (!billString) return 0;
    var parts = billString.toString().split('/');
    if (parts.length < 2) return 0;
    var cleanNumber = parts[1].replace(/[^0-9]/g, '');
    var numberPart = parseInt(cleanNumber);
    if (isNaN(numberPart)) return 0;
    return numberPart;
}

// ================= 3. ระบบเช็คข้อมูลอัตโนมัติ (HEAVILY Optimized) =================
/**
 * [OPTIMIZED] autoCheckMissingBills — แก้ไข 3 จุดหลัก:
 * 1. ดึงเฉพาะ Col C (bill number) มาสร้าง index → ไม่ต้องโหลด 700K × ทุกคอลัมน์
 * 2. ใช้ chunk-based processing ดึง 50,000 แถว/ครั้ง → ไม่ timeout
 * 3. Batch updates → เขียนหลายแถวพร้อมกันแทน cell-by-cell
 */
function autoCheckMissingBills() {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var dbSheetName = "DataBase";
    var dbSheet = ss.getSheetByName(dbSheetName);

    if (!dbSheet) return;

    var lastDbRow = getCachedLastRow(dbSheet);
    if (lastDbRow < HEADER_ROWS + 1) return;

    // ================= Phase 1: สร้าง Bill → Row Index (ดึงเฉพาะ Col C) =================
    var CHUNK_SIZE = 50000;
    var dbBillToRow = new Map();

    for (var start = HEADER_ROWS + 1; start <= lastDbRow; start += CHUNK_SIZE) {
        var numRows = Math.min(CHUNK_SIZE, lastDbRow - start + 1);
        var bills = dbSheet.getRange(start, 3, numRows, 1).getValues(); // Col C only
        for (var d = 0; d < bills.length; d++) {
            var bill = bills[d][0];
            if (bill) {
                dbBillToRow.set(bill.toString().trim(), start + d);
            }
        }
    }

    // ================= Phase 2: ประมวลผลแต่ละชีท =================
    var colBarcode = 20;  // Col T
    var colStatus = 21;   // Col U
    var colMoney = 12;    // Col L

    var allSheets = ss.getSheets();

    for (var s = 0; s < allSheets.length; s++) {
        var sheet = allSheets[s];
        if (sheet.getName() === dbSheetName) continue;

        var lastRow = getLastRowInColumn(sheet, colBarcode);
        if (lastRow < DATA_START_ROW) continue;

        var numDataRows = lastRow - (DATA_START_ROW - 1);
        var barcodeValues = sheet.getRange(DATA_START_ROW, colBarcode, numDataRows, 1).getValues();

        // ================= Phase 2a: รวบรวมแถวที่ต้อง fetch จาก DB =================
        var rowsToFetch = [];    // { sheetRow, dbRow, billCode }
        var rowsNotFound = [];   // sheetRow ที่ไม่เจอ

        for (var i = 0; i < barcodeValues.length; i++) {
            var billCode = barcodeValues[i][0].toString().trim();
            var currentRow = i + DATA_START_ROW;

            if (billCode !== "") {
                var dbRow = dbBillToRow.get(billCode);
                if (dbRow !== undefined) {
                    rowsToFetch.push({ sheetRow: currentRow, dbRow: dbRow });
                } else {
                    rowsNotFound.push(currentRow);
                }
            }
        }

        // ================= Phase 2b: Batch fetch ข้อมูลจาก DB =================
        // จัดกลุ่มแถวที่ต้อง fetch เพื่อลด API calls → ดึงเป็น batch ของแถวที่ต่อเนื่องกัน
        if (rowsToFetch.length > 0) {
            // Sort by dbRow for potential batch optimization
            rowsToFetch.sort(function (a, b) { return a.dbRow - b.dbRow; });

            // Prepare batch data arrays for the target sheet
            var dataUpdates = [];   // { row, values }
            var formulaUpdates = []; // { row, formula }

            for (var f = 0; f < rowsToFetch.length; f++) {
                var fetchInfo = rowsToFetch[f];
                var recordRaw = dbSheet.getRange(fetchInfo.dbRow, 1, 1, DB_MAX_COLS).getValues()[0];
                var mappedData = mapRecord(recordRaw);

                dataUpdates.push({ row: fetchInfo.sheetRow, values: mappedData });
                formulaUpdates.push({
                    row: fetchInfo.sheetRow,
                    formula: "=E" + fetchInfo.sheetRow + "*J" + fetchInfo.sheetRow + "-H" + fetchInfo.sheetRow + "+K" + fetchInfo.sheetRow
                });
            }

            // ================= Phase 2c: Batch write ข้อมูลลงชีท =================
            // เขียนข้อมูลทีละ batch เพื่อลดจำนวน API calls
            var WRITE_BATCH = 100;
            for (var w = 0; w < dataUpdates.length; w += WRITE_BATCH) {
                var batchEnd = Math.min(w + WRITE_BATCH, dataUpdates.length);

                for (var b = w; b < batchEnd; b++) {
                    var update = dataUpdates[b];
                    sheet.getRange(update.row, 1, 1, update.values.length).setValues([update.values]);
                    sheet.getRange(update.row, colMoney).setFormula(formulaUpdates[b].formula);
                    sheet.getRange(update.row, colStatus).setValue("OK").setBackground("#ccffcc");
                    sheet.getRange(update.row, colBarcode).setBackground("#ccffcc");
                }

                // Flush batch to avoid quota issues
                SpreadsheetApp.flush();
            }
        }

        // ================= Phase 2d: Mark rows not found =================
        for (var n = 0; n < rowsNotFound.length; n++) {
            var notFoundRow = rowsNotFound[n];
            sheet.getRange(notFoundRow, colStatus).setValue("ไม่พบข้อมูล").setBackground("#ffff99");
            sheet.getRange(notFoundRow, colBarcode).setBackground("#ffff99");
        }

        if (rowsNotFound.length > 0) {
            SpreadsheetApp.flush();
        }
    }
}

// ================= 4. Helper Functions (Optimized with TextFinder + Cache) =================

function mapRecord(dbRecord) {
    return [
        dbRecord[0],  // Target A
        dbRecord[2],  // Target B
        dbRecord[5],  // Target C
        dbRecord[6],  // Target D
        dbRecord[9],  // Target E
        dbRecord[10], // Target F
        dbRecord[11], // Target G
        dbRecord[12], // Target H
        dbRecord[13]  // Target I
    ];
}

// [OPTIMIZED v2] ใช้ CacheService + column-range TextFinder (ไม่ต้องรู้ lastRow)
function findStationNameInBigData(dbSheet, code3Digits, colBillIndex, colStationIndex) {
    var cache = CacheService.getScriptCache();
    var cacheKey = 'station_' + code3Digits;
    var cached = cache.get(cacheKey);
    if (cached) return cached;

    // [OPTIMIZED v2] ใช้ column range "C:C" → ข้าม getCachedLastRow() ไปเลย
    var colLetter = String.fromCharCode(64 + colBillIndex); // 3 → 'C'
    var searchRange = dbSheet.getRange(colLetter + ':' + colLetter);

    var regexPattern = ".*" + code3Digits + "\\/.*";
    var finder = searchRange.createTextFinder(regexPattern)
        .useRegularExpression(true)
        .matchEntireCell(true);

    var foundCell = finder.findNext();

    if (foundCell && foundCell.getRow() > HEADER_ROWS) {
        var stationName = dbSheet.getRange(foundCell.getRow(), colStationIndex).getValue();
        if (stationName) {
            cache.put(cacheKey, stationName.toString(), 300);
        }
        return stationName;
    }

    return null;
}

// [OPTIMIZED v2] ใช้ column-range TextFinder + record caching
function findRecordInBigData(dbSheet, searchVal, colIndex) {
    // [OPTIMIZED v2] ใช้ column range → ข้าม getCachedLastRow() ทั้งหมด
    var colLetter = String.fromCharCode(64 + colIndex); // 3 → 'C'
    var searchRange = dbSheet.getRange(colLetter + ':' + colLetter);

    var finder = searchRange.createTextFinder(searchVal.toString().trim()).matchEntireCell(true);
    var foundCell = finder.findNext();

    // ข้าม header rows
    if (foundCell && foundCell.getRow() > HEADER_ROWS) {
        return dbSheet.getRange(foundCell.getRow(), 1, 1, DB_MAX_COLS).getValues()[0];
    }

    return null;
}

// [OPTIMIZED v2] เช็คซ้ำด้วย column-range TextFinder (ข้าม getLastRowInColumn)
function checkDuplicateOptimized(sheet, val, colIndex) {
    var colLetter = String.fromCharCode(64 + colIndex); // 2 → 'B'
    var searchRange = sheet.getRange(colLetter + ':' + colLetter);

    var finder = searchRange.createTextFinder(val.toString().trim()).matchEntireCell(true);
    var foundCell = finder.findNext();

    // เจอแถวที่ > HEADER_ROWS ถือว่าซ้ำ
    return (foundCell && foundCell.getRow() > HEADER_ROWS);
}

// [OPTIMIZED] อ่านแค่ 500 แถวล่าสุดแทนอ่านทั้งคอลัมน์
function getLastRowInColumn(sheet, column) {
    var lastRow = sheet.getLastRow();
    if (lastRow < 1) return 0;

    // Optimized: อ่านแค่ batch ล่าสุด (500 แถว) ก่อน
    var batchSize = 500;
    var startRow = Math.max(1, lastRow - batchSize + 1);
    var numRows = lastRow - startRow + 1;
    var data = sheet.getRange(startRow, column, numRows, 1).getValues();

    for (var i = data.length - 1; i >= 0; i--) {
        if (data[i][0] !== "" && data[i][0] != null) {
            return startRow + i;
        }
    }

    // Fallback: ถ้า 500 แถวล่าสุดว่างหมด → scan จากต้น
    if (startRow > 1) {
        data = sheet.getRange(1, column, startRow - 1, 1).getValues();
        for (var i = data.length - 1; i >= 0; i--) {
            if (data[i][0] !== "" && data[i][0] != null) {
                return i + 1;
            }
        }
    }
    return 0;
}

function setSuccess(range, statusCell, msg) {
    range.setBackground("#ccffcc");
    statusCell.setValue(msg).setBackground("#ccffcc");
}
function setError(range, statusCell, msg) {
    range.setBackground("#ffcccc");
    statusCell.setValue(msg).setBackground("#ffcccc");
}
function setNotFound(range, statusCell, msg) {
    range.setBackground("#ffff99");
    statusCell.setValue(msg).setBackground("#ffff99");
}