package com.example.peetproject.controllers;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class FileDataReceiverController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final Map<String, String> dbColumnNameToSqlTypeMap = new LinkedHashMap<>();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");

    @PostMapping("/api/upload-file")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        if (file.isEmpty()) {
            return new ResponseEntity<>("Будь ласка, виберіть файл для завантаження.", HttpStatus.BAD_REQUEST);
        }

        String tableName = "package_events_queue";

        try (Reader reader = new BufferedReader(new InputStreamReader(file.getInputStream()))) {
            // Настройка CSV-парсера для корректной обработки данных
            CSVFormat csvFormat = CSVFormat.Builder.create()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setIgnoreEmptyLines(true)
                    .setTrim(true)
                    .setIgnoreSurroundingSpaces(true)
                    .setIgnoreHeaderCase(true)
                    .build();

            CSVParser csvParser = new CSVParser(reader, csvFormat);

            List<String> columnNamesFromCsvOriginalCase = csvParser.getHeaderNames().stream()
                    .map(String::trim)
                    .collect(Collectors.toList());

            if (columnNamesFromCsvOriginalCase.isEmpty()) {
                return new ResponseEntity<>("Файл пустий або не містить заголовків.", HttpStatus.BAD_REQUEST);
            }

            if (dbColumnNameToSqlTypeMap.isEmpty()) {
                fetchTableMetadata(tableName);
            }

            Map<String, String> csvColumnNameToOriginalCaseMap = new LinkedHashMap<>();
            for (String csvHeader : columnNamesFromCsvOriginalCase) {
                csvColumnNameToOriginalCaseMap.put(csvHeader.toLowerCase(), csvHeader);
            }

            List<String> validDbColumnNamesForInsert = new ArrayList<>();
            List<String> correspondingCsvColumnNames = new ArrayList<>();

            for (String dbColName : dbColumnNameToSqlTypeMap.keySet()) {
                if (csvColumnNameToOriginalCaseMap.containsKey(dbColName.toLowerCase())) {
                    validDbColumnNamesForInsert.add(dbColName);
                    correspondingCsvColumnNames.add(csvColumnNameToOriginalCaseMap.get(dbColName.toLowerCase()));
                }
            }

            if (validDbColumnNamesForInsert.isEmpty()) {
                System.err.println("Назви стовпців у БД: " + dbColumnNameToSqlTypeMap.keySet());
                return new ResponseEntity<>("Жоден із стовпців у файлі не відповідає стовпцям у таблиці БД. Перевірте регістр символів та назви стовпців.", HttpStatus.BAD_REQUEST);
            }

            // --- ИСПРАВЛЕНИЕ НАЧИНАЕТСЯ ЗДЕСЬ ---
            String quotedColumns = validDbColumnNamesForInsert.stream()
                    .map(s -> "\"" + s + "\"")
                    .collect(Collectors.joining(","));

            String placeholders = validDbColumnNamesForInsert.stream()
                    .map(s -> "?")
                    .collect(Collectors.joining(","));

            String sql = String.format("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, quotedColumns, placeholders);
            System.out.println("Сформований SQL запит (виправлений): " + sql);
            // --- ИСПРАВЛЕНИЕ ЗАКАНЧИВАЕТСЯ ЗДЕСЬ ---

            int batchSize = 100;
            List<Object[]> batchArgs = new ArrayList<>();
            long rowCount = 0;
            long errorRowCount = 0;

            for (CSVRecord csvRecord : csvParser) {
                Object[] rowValues = new Object[validDbColumnNamesForInsert.size()];
                boolean hasErrorInRow = false;

                for (int i = 0; i < validDbColumnNamesForInsert.size(); i++) {
                    String dbColumnName = validDbColumnNamesForInsert.get(i);
                    String csvColumnNameOriginalCase = correspondingCsvColumnNames.get(i);

                    String value = csvRecord.get(csvColumnNameOriginalCase);
                    String sqlType = dbColumnNameToSqlTypeMap.get(dbColumnName);

                    // Обработка специфической строки "(null)"
                    if (value == null || value.trim().isEmpty() || value.trim().equalsIgnoreCase("(null)")) {
                        rowValues[i] = null;
                        continue;
                    }

                    try {
                        String cleanedValue = value.trim();

                        if (isDateType(sqlType)) {
                            // Использование java.sql.Date для соответствия типу date в БД
                            rowValues[i] = new java.sql.Date(dateFormat.parse(cleanedValue).getTime());
                        } else if (isNumericType(sqlType)) {
                            rowValues[i] = new BigDecimal(cleanedValue);
                        } else if (isIntegerType(sqlType)) {
                            rowValues[i] = Integer.parseInt(cleanedValue);
                        } else {
                            rowValues[i] = cleanedValue;
                        }
                    } catch (NumberFormatException | ParseException e) {
                        System.err.printf("Помилка форматування даних для стовпця '%s' (тип в БД: %s), значення: '%s'. Встановлено значення null. Рядок: %d%n",
                                csvColumnNameOriginalCase, sqlType, value, csvRecord.getRecordNumber());
                        rowValues[i] = null;
                        hasErrorInRow = true;
                    }
                }

                if (!hasErrorInRow) {
                    batchArgs.add(rowValues);
                    rowCount++;
                    if (rowCount % batchSize == 0) {
                        executeBatch(sql, batchArgs, rowCount);
                        batchArgs.clear();
                    }
                } else {
                    errorRowCount++;
                }
            }

            executeBatch(sql, batchArgs, rowCount);
            String message = "Файл успішно завантажено. Записано " + rowCount + " рядків.";
            if (errorRowCount > 0) {
                message += " Пропущено " + errorRowCount + " рядків через помилки формату або невідповідність даних.";
            }
            return new ResponseEntity<>(message, HttpStatus.OK);

        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("Помилка при читанні файлу або вставці даних: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void executeBatch(String sql, List<Object[]> batchArgs, long rowCount) {
        if (!batchArgs.isEmpty()) {
            try {
                jdbcTemplate.batchUpdate(sql, batchArgs);
            } catch (Exception e) {
                System.err.printf("Помилка пакетної вставки на рядках %d-%d: %s%n",
                        (rowCount - batchArgs.size() + 1), rowCount, e.getMessage());
                // Дополнительное логирование для отладки
                // for (Object[] row : batchArgs) {
                //     System.err.println("Ошибочная строка данных: " + java.util.Arrays.toString(row));
                // }
                // Если хотите увидеть полный стектрейс, раскомментируйте
                // e.printStackTrace();
            }
        }
    }

    private void fetchTableMetadata(String tableName) throws SQLException {
        dbColumnNameToSqlTypeMap.clear();
        System.out.printf("Намагаюся отримати метадані для таблиці (через information_schema): '%s'%n", tableName);

        // SQL-запрос для получения метаданных, также цитируем название таблицы
        String sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        List<Map<String, Object>> columnsData = jdbcTemplate.queryForList(sql, "public", tableName.toLowerCase());

        if (columnsData.isEmpty()) {
            System.err.printf("Помилка: Прямий запит до information_schema.columns для таблиці '%s' (схема 'public') повернув порожній результат.%n", tableName);
            System.err.println("Можливі причини: невірне ім'я таблиці, невірний регістр, або проблема з правами доступу до information_schema.");
        }

        for (Map<String, Object> row : columnsData) {
            String columnName = ((String) row.get("column_name"));
            String dataType = (String) row.get("data_type");
            dbColumnNameToSqlTypeMap.put(columnName, dataType);
            System.out.printf("Знайдено стовпець в БД (через information_schema): %s (Тип: %s)%n", columnName, dataType);
        }

        System.out.printf("Кількість стовпців, знайдених у БД: %d%n", dbColumnNameToSqlTypeMap.size());
        System.out.printf("Назви стовпців у БД (після обробки): %s%n", dbColumnNameToSqlTypeMap.keySet());
    }

    private boolean isNumericType(String sqlType) {
        if (sqlType == null) return false;
        String lowerCaseSqlType = sqlType.toLowerCase();
        return lowerCaseSqlType.contains("numeric") ||
                lowerCaseSqlType.contains("decimal") ||
                lowerCaseSqlType.contains("real") ||
                lowerCaseSqlType.contains("double precision") ||
                lowerCaseSqlType.contains("float");
    }

    private boolean isIntegerType(String sqlType) {
        if (sqlType == null) return false;
        String lowerCaseSqlType = sqlType.toLowerCase();
        return lowerCaseSqlType.contains("int") ||
                lowerCaseSqlType.contains("serial") ||
                lowerCaseSqlType.contains("smallint") ||
                lowerCaseSqlType.contains("bigint");
    }

    private boolean isDateType(String sqlType) {
        if (sqlType == null) return false;
        return sqlType.toLowerCase().contains("date") ||
                sqlType.toLowerCase().contains("timestamp");
    }
}