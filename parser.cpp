#include "parser.h"
#include "db.h"
#include <sstream>
#include <algorithm>
#include <regex>
#include <iostream>
#include <cctype>
#include <functional>

bool parseAndExecute(const std::string &query)
{
    std::string upperQuery = query;
    std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);

    if (upperQuery.find("CREATE DATABASE") == 0)
    {
        return parseCreateDatabase(query);
    }
    else if (upperQuery.find("DROP DATABASE") == 0)
    {
        return parseDropDatabase(query);
    }
    else if (upperQuery.find("SHOW DATABASES") == 0)
    {
        return parseShowDatabases(query);
    }
    else if (upperQuery.find("SHOW TABLES") == 0)
    {
        return parseShowTables(query);
    }
    else if (upperQuery.find("DESCRIBE") == 0 || upperQuery.find("DESC") == 0)
    {
        return parseDescribe(query);
    }
    else if (upperQuery.find("DROP TABLE") == 0)
    {
        return parseDropTable(query);
    }
    else if (upperQuery.find("USE ") == 0)
    {
        return parseUse(query);
    }
    else if (upperQuery.find("CREATE TABLE") == 0)
    {
        return parseCreateTable(query);
    }
    else if (upperQuery.find("INSERT INTO") == 0)
    {
        return parseInsert(query);
    }
    else if (upperQuery.find("SELECT") == 0 || upperQuery.find("FIND") == 0)
    {
        if (upperQuery.find("JOIN") != std::string::npos)
        {
            return parseJoin(query);
        }
        else if (upperQuery.find("SUM") != std::string::npos ||
                 upperQuery.find("AVG") != std::string::npos ||
                 upperQuery.find("COUNT") != std::string::npos ||
                 upperQuery.find("MAX") != std::string::npos ||
                 upperQuery.find("MIN") != std::string::npos)
        {
            return parseAggregate(query);
        }
        else
        {
            return parseSelect(query);
        }
    }
    else if (upperQuery.find("UPDATE") == 0)
    {
        return parseUpdate(query);
    }
    else if (upperQuery.find("DELETE FROM") == 0 || upperQuery.find("KILL FROM") == 0)
    {
        return parseDelete(query);
    }
    else if (upperQuery.find("CREATE INDEX") == 0)
    {
        return parseCreateIndex(query);
    }
    else if (upperQuery.find("EXPORT TO CSV") == 0)
    {
        return parseExport(query);
    }
    else if (upperQuery.find("BACKUP") == 0)
    {
        return parseBackup(query);
    }
    else if (upperQuery.find("RESTORE") == 0)
    {
        return parseRestore(query);
    }
    else if (upperQuery.find("BEGIN") == 0)
    {
        return beginTransaction();
    }
    else if (upperQuery.find("COMMIT") == 0)
    {
        return commitTransaction();
    }
    else if (upperQuery.find("SAVE") == 0)
    {
        if (!currentDatabase.empty())
        {
            saveToFile(currentDatabase);
            std::cout << "Database '" << currentDatabase << "' saved successfully!" << std::endl;
        }
        else
        {
            std::cout << "No database selected!" << std::endl;
        }
        return true;
    }
    else if (upperQuery.find("ROLLBACK") == 0)
    {
        return rollbackTransaction();
    }
    else
    {
        std::cout << "Unknown command: " << query << std::endl;
        return false;
    }
}

bool parseCreateDatabase(const std::string &query)
{
    std::regex dbRegex(R"(CREATE\s+DATABASE\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, dbRegex))
    {
        std::string dbName = match[1].str();
        return createDatabase(dbName);
    }
    std::cout << "Error: Invalid CREATE DATABASE syntax" << std::endl;
    return false;
}

bool parseDropDatabase(const std::string &query)
{
    std::regex dbRegex(R"(DROP\s+DATABASE\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, dbRegex))
    {
        std::string dbName = match[1].str();
        return dropDatabase(dbName);
    }
    std::cout << "Error: Invalid DROP DATABASE syntax" << std::endl;
    return false;
}

bool parseShowDatabases(const std::string &query)
{
    listDatabases();
    return true;
}

bool parseShowTables(const std::string &query)
{
    showTables();
    return true;
}

bool parseDescribe(const std::string &query)
{
    std::regex descRegex(R"((?:DESCRIBE|DESC)\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, descRegex))
    {
        std::string tableName = match[1].str();
        describeTable(tableName);
        return true;
    }
    return false;
}

bool parseDropTable(const std::string &query)
{
    std::regex dropRegex(R"(DROP\s+TABLE\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, dropRegex))
    {
        std::string tableName = match[1].str();
        if (tables.erase(tableName) > 0)
        {
            std::cout << "Table '" << tableName << "' dropped successfully" << std::endl;
            return true;
        }
        std::cout << "Error: Table '" << tableName << "' does not exist" << std::endl;
        return false;
    }
    return false;
}

bool parseUse(const std::string &query)
{
    std::regex useRegex(R"(USE\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, useRegex))
    {
        return useDatabase(match[1].str());
    }
    return false;
}

bool parseCreateTable(const std::string &query)
{
    std::regex tableRegex(R"(CREATE\s+TABLE\s+(\w+)\s*\((.+)\);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, tableRegex))
    {
        std::string tableName = match[1].str();
        std::string columnsStr = match[2].str();

        std::vector<Column> columns;
        std::stringstream ss(columnsStr);
        std::string columnDef;

        while (std::getline(ss, columnDef, ','))
        {
            Column col;
            std::stringstream colStream(columnDef);
            std::string token;
            std::vector<std::string> tokens;

            while (colStream >> token)
            {
                tokens.push_back(token);
            }

            if (tokens.size() < 2)
                continue;

            col.name = tokens[0];

            // Parse type
            std::string typeStr = tokens[1];
            if (typeStr == "INT" || typeStr == "INTEGER")
            {
                col.type = DataType::INT;
                col.length = 11;
            }
            else if (typeStr.substr(0, 7) == "VARCHAR")
            {
                col.type = DataType::STRING;
                std::regex lengthRegex(R"(VARCHAR\((\d+)\))");
                std::smatch lenMatch;
                if (std::regex_search(typeStr, lenMatch, lengthRegex))
                {
                    col.length = std::stoi(lenMatch[1].str());
                }
                else
                {
                    col.length = 255;
                }
            }
            else if (typeStr == "TEXT")
            {
                col.type = DataType::STRING;
                col.length = 65535;
            }
            else if (typeStr == "FLOAT" || typeStr == "REAL")
            {
                col.type = DataType::FLOAT;
                col.length = 8;
            }
            else if (typeStr == "BOOLEAN" || typeStr == "BOOL")
            {
                col.type = DataType::BOOL;
                col.length = 1;
            }
            else if (typeStr == "DATE")
            {
                col.type = DataType::DATE;
                col.length = 10;
            }

            col.notNull = false;
            col.isPrimaryKey = false;
            col.isForeignKey = false;
            col.isUnique = false;
            col.autoIncrement = false;

            for (size_t i = 2; i < tokens.size(); i++)
            {
                std::string constraint = tokens[i];
                if (constraint == "NOT" && i + 1 < tokens.size() && tokens[i + 1] == "NULL")
                {
                    col.notNull = true;
                    i++;
                }
                else if (constraint == "PRIMARY" && i + 1 < tokens.size() && tokens[i + 1] == "KEY")
                {
                    col.isPrimaryKey = true;
                    i++;
                }
                else if (constraint == "UNIQUE")
                {
                    col.isUnique = true;
                }
                else if (constraint == "AUTO_INCREMENT")
                {
                    col.autoIncrement = true;
                }
                else if (constraint == "REFERENCES" && i + 2 < tokens.size())
                {
                    col.isForeignKey = true;
                    col.referencesTable = tokens[i + 1];
                    std::string refCol = tokens[i + 2];
                    if (refCol.front() == '(')
                        refCol = refCol.substr(1);
                    if (refCol.back() == ')')
                        refCol = refCol.substr(0, refCol.length() - 1);
                    col.referencesColumn = refCol;
                    i += 2;
                }
            }

            columns.push_back(col);
        }

        return createTable(tableName, columns);
    }
    return false;
}

bool parseInsert(const std::string &query)
{
    std::regex insertRegex(R"(INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, insertRegex))
    {
        std::string tableName = match[1].str();
        std::string valuesStr = match[2].str();

        Row row;
        std::stringstream ss(valuesStr);
        std::string value;
        int colIndex = 0;

        auto tableIt = tables.find(tableName);
        if (tableIt == tables.end())
        {
            std::cout << "Table not found" << std::endl;
            return false;
        }

        while (std::getline(ss, value, ','))
        {
            // Trim whitespace and quotes
            value.erase(0, value.find_first_not_of(" \t\n\r"));
            value.erase(value.find_last_not_of(" \t\n\r") + 1);

            if (colIndex >= (int)tableIt->second->columns.size())
                break;

            std::string colName = tableIt->second->columns[colIndex].name;
            DataType colType = tableIt->second->columns[colIndex].type;

            if (value == "NULL")
            {
                row.values[colName] = std::any();
            }
            else if (value.front() == '"' || value.front() == '\'')
            {
                value = value.substr(1, value.length() - 2);
                row.values[colName] = std::any(value);
            }
            else if (colType == DataType::INT)
            {
                row.values[colName] = std::any(std::stoi(value));
            }
            else if (colType == DataType::FLOAT)
            {
                row.values[colName] = std::any(std::stof(value));
            }
            else if (colType == DataType::BOOL)
            {
                row.values[colName] = std::any(value == "TRUE" || value == "1" || value == "true");
            }
            else
            {
                row.values[colName] = std::any(value);
            }
            colIndex++;
        }

        return insertInto(tableName, row);
    }
    return false;
}

bool parseSelect(const std::string &query)
{
    std::regex selectRegex(R"((?:SELECT|FIND)\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?(?:\s+ORDER\s+BY\s+(\w+)\s+(ASC|DESC))?;?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, selectRegex))
    {
        std::string columnsStr = match[1].str();
        std::string tableName = match[2].str();
        std::string condition = "";

        if (match.size() > 3 && match[3].matched)
        {
            condition = match[3].str();

            // Remove trailing semicolon if present
            size_t semicolonPos = condition.find(';');
            if (semicolonPos != std::string::npos)
            {
                condition = condition.substr(0, semicolonPos);
            }

            // Trim whitespace
            size_t first = condition.find_first_not_of(" \t\n\r");
            size_t last = condition.find_last_not_of(" \t\n\r");
            if (first != std::string::npos && last != std::string::npos)
            {
                condition = condition.substr(first, last - first + 1);
            }
        }

        std::string orderBy = "";
        std::string orderDir = "ASC";
        if (match.size() > 4 && match[4].matched)
        {
            orderBy = match[4].str();
            orderDir = match[5].str();
        }

        std::vector<std::string> columns;
        if (columnsStr == "*")
        {
            columns.push_back("*");
        }
        else
        {
            std::stringstream ss(columnsStr);
            std::string col;
            while (std::getline(ss, col, ','))
            {
                // Trim whitespace
                size_t first = col.find_first_not_of(" \t\n\r");
                size_t last = col.find_last_not_of(" \t\n\r");
                if (first != std::string::npos && last != std::string::npos)
                {
                    col = col.substr(first, last - first + 1);
                }
                columns.push_back(col);
            }
        }

        QueryResult result = select(tableName, columns, condition, orderBy, orderDir);
        printTable(result);
        return true;
    }
    return false;
}

bool parseUpdate(const std::string &query)
{
    std::regex updateRegex(R"(UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?;?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, updateRegex))
    {
        std::string tableName = match[1].str();
        std::string setClause = match[2].str();
        std::string condition = match.size() > 3 ? match[3].str() : "";

        std::map<std::string, std::any> setValues;
        std::stringstream ss(setClause);
        std::string assignment;

        while (std::getline(ss, assignment, ','))
        {
            size_t eqPos = assignment.find('=');
            if (eqPos != std::string::npos)
            {
                std::string col = assignment.substr(0, eqPos);
                std::string val = assignment.substr(eqPos + 1);

                col.erase(0, col.find_first_not_of(" \t\n\r"));
                col.erase(col.find_last_not_of(" \t\n\r") + 1);
                val.erase(0, val.find_first_not_of(" \t\n\r"));
                val.erase(val.find_last_not_of(" \t\n\r") + 1);

                if (val.front() == '"' || val.front() == '\'')
                {
                    val = val.substr(1, val.length() - 2);
                    setValues[col] = std::any(val);
                }
                else
                {
                    setValues[col] = std::any(std::stoi(val));
                }
            }
        }

        return updateRecords(tableName, setValues, condition);
    }
    return false;
}

bool parseDelete(const std::string &query)
{
    std::regex deleteRegex(R"((?:DELETE|KILL)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?;?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, deleteRegex))
    {
        std::string tableName = match[1].str();
        std::string condition = match.size() > 2 ? match[2].str() : "";

        return deleteRecords(tableName, condition);
    }
    return false;
}

bool parseCreateIndex(const std::string &query)
{
    std::regex indexRegex(R"(CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\((\w+)\);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, indexRegex))
    {
        std::string indexName = match[1].str();
        std::string tableName = match[2].str();
        std::string columnName = match[3].str();

        return createIndex(tableName, columnName);
    }
    return false;
}

bool parseJoin(const std::string &query)
{
    // Updated regex to handle different JOIN syntaxes
    std::regex joinRegex(R"(SELECT\s+(.+?)\s+FROM\s+(\w+)\s+(INNER|LEFT|RIGHT)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)(?:\s+WHERE\s+(.+))?;?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, joinRegex))
    {
        std::string columnsStr = match[1].str();
        std::string table1 = match[2].str();
        std::string joinType = match[3].str();
        std::string table2 = match[4].str();
        std::string col1Table = match[5].str();
        std::string col1 = match[6].str();
        std::string col2Table = match[7].str();
        std::string col2 = match[8].str();
        std::string condition = match.size() > 9 ? match[9].str() : "";

        // Parse columns
        std::vector<std::string> columns;
        std::stringstream ss(columnsStr);
        std::string col;
        while (std::getline(ss, col, ','))
        {
            col.erase(0, col.find_first_not_of(" \t\n\r"));
            col.erase(col.find_last_not_of(" \t\n\r") + 1);
            columns.push_back(col);
        }

        JoinCondition join;
        join.table1 = table1;
        join.table2 = table2;
        join.column1 = col1;
        join.column2 = col2;
        join.joinType = joinType;

        std::cout << "\nExecuting " << joinType << " JOIN between " << table1 << " and " << table2 << std::endl;
        std::cout << "ON " << table1 << "." << col1 << " = " << table2 << "." << col2 << std::endl;

        QueryResult result = executeJoin(join, columns, condition);
        printTable(result);
        return true;
    }

    // Alternative syntax without explicit JOIN type (default INNER)
    std::regex joinRegex2(R"(SELECT\s+(.+?)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)(?:\s+WHERE\s+(.+))?;?)", std::regex::icase);
    if (std::regex_search(query, match, joinRegex2))
    {
        std::string columnsStr = match[1].str();
        std::string table1 = match[2].str();
        std::string table2 = match[3].str();
        std::string col1Table = match[4].str();
        std::string col1 = match[5].str();
        std::string col2Table = match[6].str();
        std::string col2 = match[7].str();
        std::string condition = match.size() > 8 ? match[8].str() : "";

        std::vector<std::string> columns;
        std::stringstream ss(columnsStr);
        std::string col;
        while (std::getline(ss, col, ','))
        {
            col.erase(0, col.find_first_not_of(" \t\n\r"));
            col.erase(col.find_last_not_of(" \t\n\r") + 1);
            columns.push_back(col);
        }

        JoinCondition join;
        join.table1 = table1;
        join.table2 = table2;
        join.column1 = col1;
        join.column2 = col2;
        join.joinType = "INNER";

        QueryResult result = executeJoin(join, columns, condition);
        printTable(result);
        return true;
    }

    std::cout << "Error: Invalid JOIN syntax" << std::endl;
    std::cout << "Correct syntax: SELECT columns FROM table1 JOIN table2 ON table1.col = table2.col" << std::endl;
    return false;
}

bool parseAggregate(const std::string &query)
{
    std::regex aggRegex(R"(SELECT\s+(SUM|AVG|COUNT|MAX|MIN)\((\w+)\)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?;?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, aggRegex))
    {
        std::string function = match[1].str();
        std::string column = match[2].str();
        std::string tableName = match[3].str();
        std::string condition = match.size() > 4 ? match[4].str() : "";

        QueryResult result = aggregateFunction(function, tableName, column, condition);
        printTable(result);
        return true;
    }
    return false;
}

bool parseExport(const std::string &query)
{
    std::regex exportRegex(R"(EXPORT\s+TO\s+CSV\s+(\w+)\s+FROM\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, exportRegex))
    {
        std::string filename = match[1].str();
        std::string tableName = match[2].str();

        return exportToCSV(tableName, filename);
    }
    return false;
}

bool parseBackup(const std::string &query)
{
    std::regex backupRegex(R"(BACKUP\s+DATABASE\s+TO\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, backupRegex))
    {
        std::string backupName = match[1].str();
        return backupDatabase(backupName);
    }
    return false;
}

bool parseRestore(const std::string &query)
{
    std::regex restoreRegex(R"(RESTORE\s+DATABASE\s+FROM\s+(\w+);?)", std::regex::icase);
    std::smatch match;

    if (std::regex_search(query, match, restoreRegex))
    {
        std::string backupName = match[1].str();
        return restoreDatabase(backupName);
    }
    return false;
}