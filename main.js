// ================= ตั้งค่าส่วนกลาง (Global Config) =================
var DATA_START_ROW = 3; // ข้อมูลเริ่มที่แถว 3
var HEADER_ROWS = 2;    // จำนวนแถวหัวตาราง
var DB_MAX_COLS = 14;   // ดึงเฉพาะ Col A-N (14 คอลัมน์) ที่ใช้จริง
var INDEX_SHEET_NAME = "BillIndex"; // ชีท index สำหรับ binary search
var INDEX_CHUNK_SIZE = 10000; // จำนวน entries ต่อ 1 chunk ใน boundary cache

// ================= Cache Helpers =================

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
 * Cache ข้อมูล record ของ bill ที่เคยค้นหาแล้ว
 * ยิงบาร์โค้ดซ้ำ / ค้นหา bill เดิม → ดึงจาก cache ทันที (< 100ms)
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

// =================================================================================
// ================= INDEX SHEET + BINARY SEARCH SYSTEM (v3) =======================
// =================================================================================

/**
 * สร้าง/rebuild ชีท "BillIndex" จาก DataBase
 * ดึง Bill Number (Col C) + Row Number → sort → เขียนลง BillIndex
 * 
 * ⚠️ ตั้ง Time-driven Trigger ให้รันทุก 10 นาที:
 *    Apps Script Editor → Triggers → Add Trigger → rebuildBillIndex → Time-driven → Every 10 minutes
 */
function rebuildBillIndex() {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var dbSheet = ss.getSheetByName("DataBase");
    if (!dbSheet) return;

    var lastDbRow = dbSheet.getLastRow();
    if (lastDbRow < HEADER_ROWS + 1) return;

    // ================= Phase 1: ดึง Bill Numbers + Row Numbers =================
    var CHUNK_SIZE = 50000;
    var indexData = []; // [[bill, rowNumber], ...]

    for (var start = HEADER_ROWS + 1; start <= lastDbRow; start += CHUNK_SIZE) {
        var numRows = Math.min(CHUNK_SIZE, lastDbRow - start + 1);
        var bills = dbSheet.getRange(start, 3, numRows, 1).getValues(); // Col C only

        for (var d = 0; d < bills.length; d++) {
            var bill = bills[d][0];
            if (bill && bill.toString().trim() !== "") {
                indexData.push([bill.toString().trim(), start + d]);
            }
        }
    }

    if (indexData.length === 0) return;

    // ================= Phase 2: Sort ตาม Bill Number =================
    indexData.sort(function (a, b) {
        var billA = a[0].toString();
        var billB = b[0].toString();
        if (billA < billB) return -1;
        if (billA > billB) return 1;
        return 0;
    });

    // ================= Phase 3: เขียนลง BillIndex Sheet =================
    var indexSheet = ss.getSheetByName(INDEX_SHEET_NAME);
    if (!indexSheet) {
        indexSheet = ss.insertSheet(INDEX_SHEET_NAME);
    } else {
        indexSheet.clearContents();
    }

    // Header
    indexSheet.getRange(1, 1, 1, 2).setValues([["BillNumber", "RowInDB"]]);

    // เขียนข้อมูลเป็น chunk เพื่อหลีกเลี่ยง timeout
    var WRITE_CHUNK = 50000;
    for (var w = 0; w < indexData.length; w += WRITE_CHUNK) {
        var chunk = indexData.slice(w, Math.min(w + WRITE_CHUNK, indexData.length));
        indexSheet.getRange(w + 2, 1, chunk.length, 2).setValues(chunk);
    }

    // ================= Phase 4: Cache Boundary List =================
    cacheBoundaries(indexData);

    // ซ่อน sheet ไม่ให้ user เห็น (optional)
    indexSheet.hideSheet();

    Logger.log("✅ BillIndex rebuilt: " + indexData.length + " entries");
}

/**
 * Cache ทุก INDEX_CHUNK_SIZE entries จาก sorted index เป็น "boundaries"
 * ใช้สำหรับ binary search หา chunk range
 */
function cacheBoundaries(indexData) {
    var cache = CacheService.getScriptCache();
    var boundaries = [];

    // เก็บ entry ทุก INDEX_CHUNK_SIZE entries
    for (var i = 0; i < indexData.length; i += INDEX_CHUNK_SIZE) {
        boundaries.push(indexData[i][0]); // bill number ตัวแรกของแต่ละ chunk
    }

    // Cache boundaries เป็น JSON (70 entries × ~20 bytes = ~1.4KB → สบาย)
    cache.put('idx_boundaries', JSON.stringify(boundaries), 900); // 15 นาที
    cache.put('idx_total', indexData.length.toString(), 900);

    Logger.log("Cached " + boundaries.length + " boundaries");
}

/**
 * ค้นหา record จาก BillIndex ด้วย Binary Search
 * 1. ดึง boundaries จาก cache → หา chunk ที่ต้องอ่าน
 * 2. อ่าน chunk ~10K entries จาก BillIndex
 * 3. Binary search ใน chunk → ได้ row number ใน DataBase
 * 4. ดึง 1 แถวจาก DataBase
 */
function findRecordByIndex(ss, dbSheet, billNumber) {
    var cache = CacheService.getScriptCache();
    var billTrimmed = billNumber.toString().trim();

    // ================= Step 1: ดึง boundaries จาก cache =================
    var boundariesJson = cache.get('idx_boundaries');
    var totalStr = cache.get('idx_total');

    if (!boundariesJson || !totalStr) {
        // ไม่มี index → fallback ไปใช้ TextFinder
        return findRecordInBigData(dbSheet, billTrimmed, 3);
    }

    var boundaries = JSON.parse(boundariesJson);
    var totalEntries = parseInt(totalStr);

    // ================= Step 2: Binary search ใน boundaries =================
    // หา chunk index ที่ bill number อยู่
    var chunkIndex = binarySearchBoundary(boundaries, billTrimmed);

    // ================= Step 3: โหลด chunk จาก BillIndex sheet =================
    var indexSheet = ss.getSheetByName(INDEX_SHEET_NAME);
    if (!indexSheet) {
        return findRecordInBigData(dbSheet, billTrimmed, 3);
    }

    // คำนวณ range ของ chunk ที่ต้องอ่าน (row ใน BillIndex sheet, +2 เพราะ row 1 = header)
    var chunkStartRow = chunkIndex * INDEX_CHUNK_SIZE + 2; // +2: header อยู่ row 1, data เริ่ม row 2
    var chunkEndRow = Math.min(chunkStartRow + INDEX_CHUNK_SIZE - 1, totalEntries + 1);
    var chunkNumRows = chunkEndRow - chunkStartRow + 1;

    if (chunkNumRows <= 0) {
        return findRecordInBigData(dbSheet, billTrimmed, 3);
    }

    var chunkData = indexSheet.getRange(chunkStartRow, 1, chunkNumRows, 2).getValues();

    // ================= Step 4: Binary search ใน chunk =================
    var dbRowNumber = binarySearchInChunk(chunkData, billTrimmed);

    if (dbRowNumber === -1) {
        // ไม่เจอใน chunk → fallback ลองเช็ค chunk ข้าง ๆ หรือ TextFinder
        return findRecordInBigData(dbSheet, billTrimmed, 3);
    }

    // ================= Step 5: ดึง record จาก DataBase =================
    return dbSheet.getRange(dbRowNumber, 1, 1, DB_MAX_COLS).getValues()[0];
}

/**
 * Binary search ใน boundary list → return chunk index ที่ bill อาจอยู่
 */
function binarySearchBoundary(boundaries, target) {
    var low = 0;
    var high = boundaries.length - 1;

    while (low <= high) {
        var mid = Math.floor((low + high) / 2);
        var cmp = boundaries[mid].toString();

        if (cmp === target) return mid;
        if (cmp < target) low = mid + 1;
        else high = mid - 1;
    }

    // target อยู่ระหว่าง boundaries → return chunk ก่อนหน้า
    return Math.max(0, low - 1);
}

/**
 * Binary search ใน chunk data [[bill, rowNumber], ...] → return row number ใน DB
 * Return -1 ถ้าไม่เจอ
 */
function binarySearchInChunk(chunkData, target) {
    var low = 0;
    var high = chunkData.length - 1;

    while (low <= high) {
        var mid = Math.floor((low + high) / 2);
        var bill = chunkData[mid][0].toString().trim();

        if (bill === target) return chunkData[mid][1]; // row number in DB
        if (bill < target) low = mid + 1;
        else high = mid - 1;
    }

    return -1; // not found
}

// ================= 1. onEdit Trigger (Optimized v3 — Index + Binary Search) =================
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

    if (sheetName === dbSheetName || sheetName === INDEX_SHEET_NAME) return;

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
    // ส่วนที่ 2: ระบบยิงบาร์โค้ด (v3 — Index + Binary Search)
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
            // เช็คซ้ำก่อน fetch → fail-fast
            if (checkDuplicateOptimized(sheet, val, 2)) {
                setError(range, statusCell, "ข้อมูลซ้ำ");
                sheet.getRange(row, 1, 1, 9).clearContent();
                range.activate();
                return;
            }

            // ===== v3: ลองดึงจาก record cache → Index+BinarySearch → fallback TextFinder =====
            var recordRaw = getCachedRecord(val);
            if (!recordRaw) {
                recordRaw = findRecordByIndex(ss, dbSheet, val);
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

// ================= ระบบเช็คข้อมูลอัตโนมัติ (ใช้ Index) =================
/**
 * autoCheckMissingBills — ใช้ BillIndex เป็น Map แทนโหลดจาก DataBase ตรง
 * ถ้าไม่มี BillIndex → fallback สร้าง Map จาก DataBase โดยตรง
 */
function autoCheckMissingBills() {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var dbSheetName = "DataBase";
    var dbSheet = ss.getSheetByName(dbSheetName);

    if (!dbSheet) return;

    // ลองใช้ BillIndex sheet ก่อน (เร็วกว่า: sorted + 2 columns only)
    var indexSheet = ss.getSheetByName(INDEX_SHEET_NAME);
    var dbBillToRow = new Map();

    if (indexSheet && indexSheet.getLastRow() > 1) {
        // ดึงจาก BillIndex (2 columns: bill, rowNumber) → เร็วกว่าดึงจาก DataBase
        var lastIdxRow = indexSheet.getLastRow();
        var CHUNK_SIZE = 50000;

        for (var start = 2; start <= lastIdxRow; start += CHUNK_SIZE) {
            var numRows = Math.min(CHUNK_SIZE, lastIdxRow - start + 1);
            var idxData = indexSheet.getRange(start, 1, numRows, 2).getValues();
            for (var d = 0; d < idxData.length; d++) {
                if (idxData[d][0]) {
                    dbBillToRow.set(idxData[d][0].toString().trim(), idxData[d][1]);
                }
            }
        }
    } else {
        // Fallback: ดึงจาก DataBase โดยตรง (Col C)
        var lastDbRow = getCachedLastRow(dbSheet);
        if (lastDbRow < HEADER_ROWS + 1) return;

        var CHUNK_SIZE = 50000;
        for (var start = HEADER_ROWS + 1; start <= lastDbRow; start += CHUNK_SIZE) {
            var numRows = Math.min(CHUNK_SIZE, lastDbRow - start + 1);
            var bills = dbSheet.getRange(start, 3, numRows, 1).getValues();
            for (var d = 0; d < bills.length; d++) {
                var bill = bills[d][0];
                if (bill) {
                    dbBillToRow.set(bill.toString().trim(), start + d);
                }
            }
        }
    }

    // ================= ประมวลผลแต่ละชีท =================
    var colBarcode = 20;
    var colStatus = 21;
    var colMoney = 12;

    var allSheets = ss.getSheets();

    for (var s = 0; s < allSheets.length; s++) {
        var sheet = allSheets[s];
        var sName = sheet.getName();
        if (sName === dbSheetName || sName === INDEX_SHEET_NAME) continue;

        var lastRow = getLastRowInColumn(sheet, colBarcode);
        if (lastRow < DATA_START_ROW) continue;

        var numDataRows = lastRow - (DATA_START_ROW - 1);
        var barcodeValues = sheet.getRange(DATA_START_ROW, colBarcode, numDataRows, 1).getValues();

        var rowsToFetch = [];
        var rowsNotFound = [];

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

        if (rowsToFetch.length > 0) {
            rowsToFetch.sort(function (a, b) { return a.dbRow - b.dbRow; });

            var dataUpdates = [];
            var formulaUpdates = [];

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

                SpreadsheetApp.flush();
            }
        }

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

// ================= Helper Functions (Search) =================

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

// ใช้ CacheService + column-range TextFinder หา station name
function findStationNameInBigData(dbSheet, code3Digits, colBillIndex, colStationIndex) {
    var cache = CacheService.getScriptCache();
    var cacheKey = 'station_' + code3Digits;
    var cached = cache.get(cacheKey);
    if (cached) return cached;

    var colLetter = String.fromCharCode(64 + colBillIndex);
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

// Fallback: TextFinder ค้นหาตรงใน DataBase (ใช้เมื่อไม่มี BillIndex)
function findRecordInBigData(dbSheet, searchVal, colIndex) {
    var colLetter = String.fromCharCode(64 + colIndex);
    var searchRange = dbSheet.getRange(colLetter + ':' + colLetter);

    var finder = searchRange.createTextFinder(searchVal.toString().trim()).matchEntireCell(true);
    var foundCell = finder.findNext();

    if (foundCell && foundCell.getRow() > HEADER_ROWS) {
        return dbSheet.getRange(foundCell.getRow(), 1, 1, DB_MAX_COLS).getValues()[0];
    }

    return null;
}

// เช็คซ้ำด้วย column-range TextFinder
function checkDuplicateOptimized(sheet, val, colIndex) {
    var colLetter = String.fromCharCode(64 + colIndex);
    var searchRange = sheet.getRange(colLetter + ':' + colLetter);

    var finder = searchRange.createTextFinder(val.toString().trim()).matchEntireCell(true);
    var foundCell = finder.findNext();

    return (foundCell && foundCell.getRow() > HEADER_ROWS);
}

// อ่านแค่ 500 แถวล่าสุดเพื่อหา last row ที่มีข้อมูล
function getLastRowInColumn(sheet, column) {
    var lastRow = sheet.getLastRow();
    if (lastRow < 1) return 0;

    var batchSize = 500;
    var startRow = Math.max(1, lastRow - batchSize + 1);
    var numRows = lastRow - startRow + 1;
    var data = sheet.getRange(startRow, column, numRows, 1).getValues();

    for (var i = data.length - 1; i >= 0; i--) {
        if (data[i][0] !== "" && data[i][0] != null) {
            return startRow + i;
        }
    }

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