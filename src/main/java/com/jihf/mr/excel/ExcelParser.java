package com.jihf.mr.excel;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
/**
 * Func：hadoop 解析excel
 * Desc:
 * Author：JHF
 * Data：2017-08-16 11:41
 * Mail：jihaifeng@raiyi.com
 */
public class ExcelParser {
    private static final Log logger = LogFactory.getLog(ExcelParser.class);

    /**
     * 解析is
     * 
     * @param is 数据源
     * @return String[]
     */
    public static String[] parseExcelData(InputStream is) {
        // 结果集
        List<String> resultList = new ArrayList<String>();
        
        try {
            // 获取Workbook
            HSSFWorkbook workbook = new HSSFWorkbook(is);
            // 获取sheet
            HSSFSheet sheet = workbook.getSheetAt(0);
            
            Iterator<Row> rowIterator = sheet.iterator();
            
            while (rowIterator.hasNext()) {
                // 行
                Row row = rowIterator.next();
                // 字符串
                StringBuilder rowString = new StringBuilder();
                
                Iterator<Cell> colIterator = row.cellIterator();
                while (colIterator.hasNext()) {
                    Cell cell = colIterator.next();

                    switch (cell.getCellType()) {
                        case Cell.CELL_TYPE_BOOLEAN:
                            rowString.append(cell.getBooleanCellValue() + "\t");
                            break;
                        case Cell.CELL_TYPE_NUMERIC:
                            rowString.append(cell.getNumericCellValue() + "\t");
                            break;
                        case Cell.CELL_TYPE_STRING:
                            rowString.append(cell.getStringCellValue() + "\t");
                            break;
                    }
                }
                
                resultList.add(rowString.toString());
            }
        } catch (IOException e) {
            logger.error("IO Exception : File not found " + e);
        }
        
        return resultList.toArray(new String[0]);
    }
}