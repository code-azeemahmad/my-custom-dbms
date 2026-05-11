#include "db.h"
#include <fstream>
#include <iostream>
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <cmath>
#include <regex>
#include <cctype>

namespace fs = std::filesystem;

std::string trim(const std::string &str)
{
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos)
        return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

void debugCondition(const std::string &condition)
{
    std::cout << "DEBUG: Evaluating condition: '" << condition << "'" << std::endl;
}

bool createTable(const std::string &name, const std::vector<Column> &columns)
{
    if (tables.find(name) != tables.end())
    {
        std::cout << "Error: Table " << name << " already exists" << std::endl;
        return false;
    }

    auto table = std::make_unique<Table>(name);
    table->columns = columns;

    // Set auto-increment for primary key if needed
    for (auto &col : table->columns)
    {
        if (col.isPrimaryKey && col.autoIncrement)
        {
            // Primary key will be auto-generated
        }
    }

    tables[name] = std::move(table);

    std::cout << "Table '" << name << "' created successfully" << std::endl;
    return true;
}

// Add this function to db.cpp - Complete JOIN implementation
QueryResult executeJoin(const JoinCondition &join, const std::vector<std::string> &columns, const std::string &condition)
{
    QueryResult result;

    if (tables.find(join.table1) == tables.end())
    {
        std::cout << "Error: Table '" << join.table1 << "' does not exist" << std::endl;
        return result;
    }
    if (tables.find(join.table2) == tables.end())
    {
        std::cout << "Error: Table '" << join.table2 << "' does not exist" << std::endl;
        return result;
    }

    auto &table1 = tables[join.table1];
    auto &table2 = tables[join.table2];

    // Parse requested columns
    std::vector<std::pair<std::string, std::string>> columnMap; // table, column
    bool selectAll = false;

    if (columns.size() == 1 && columns[0] == "*")
    {
        selectAll = true;
        // Add all columns from both tables
        for (const auto &col : table1->columns)
        {
            columnMap.push_back({join.table1, col.name});
            result.columns.push_back(join.table1 + "." + col.name);
        }
        for (const auto &col : table2->columns)
        {
            columnMap.push_back({join.table2, col.name});
            result.columns.push_back(join.table2 + "." + col.name);
        }
    }
    else
    {
        for (const auto &col : columns)
        {
            size_t dotPos = col.find('.');
            if (dotPos != std::string::npos)
            {
                std::string table = col.substr(0, dotPos);
                std::string column = col.substr(dotPos + 1);
                columnMap.push_back({table, column});
                result.columns.push_back(col);
            }
            else
            {
                // Try to find which table has this column
                bool found = false;
                for (const auto &tableCol : table1->columns)
                {
                    if (tableCol.name == col)
                    {
                        columnMap.push_back({join.table1, col});
                        result.columns.push_back(join.table1 + "." + col);
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    for (const auto &tableCol : table2->columns)
                    {
                        if (tableCol.name == col)
                        {
                            columnMap.push_back({join.table2, col});
                            result.columns.push_back(join.table2 + "." + col);
                            found = true;
                            break;
                        }
                    }
                }
                if (!found)
                {
                    std::cout << "Error: Column '" << col << "' not found in any table" << std::endl;
                    return result;
                }
            }
        }
    }

    // Perform the join
    for (const auto &row1 : table1->data)
    {
        for (const auto &row2 : table2->data)
        {
            bool joinConditionMet = false;

            // Check join condition
            if (row1.values.find(join.column1) != row1.values.end() &&
                row2.values.find(join.column2) != row2.values.end())
            {

                auto val1 = row1.values.at(join.column1);
                auto val2 = row2.values.at(join.column2);

                // Compare values based on type
                if (val1.type() == typeid(int) && val2.type() == typeid(int))
                {
                    joinConditionMet = (std::any_cast<int>(val1) == std::any_cast<int>(val2));
                }
                else if (val1.type() == typeid(std::string) && val2.type() == typeid(std::string))
                {
                    joinConditionMet = (std::any_cast<std::string>(val1) == std::any_cast<std::string>(val2));
                }
                else if (val1.type() == typeid(float) && val2.type() == typeid(float))
                {
                    joinConditionMet = (std::any_cast<float>(val1) == std::any_cast<float>(val2));
                }
            }

            if (join.joinType == "INNER" && joinConditionMet)
            {
                std::map<std::string, std::any> resultRow;

                for (const auto &[table, column] : columnMap)
                {
                    if (table == join.table1 && row1.values.find(column) != row1.values.end())
                    {
                        resultRow[table + "." + column] = row1.values.at(column);
                    }
                    else if (table == join.table2 && row2.values.find(column) != row2.values.end())
                    {
                        resultRow[table + "." + column] = row2.values.at(column);
                    }
                }

                // Apply additional WHERE condition if any
                if (condition.empty() || evaluateConditionOnJoinRow(resultRow, condition))
                {
                    result.rows.push_back(resultRow);
                }
            }
            else if (join.joinType == "LEFT" && joinConditionMet)
            {
                std::map<std::string, std::any> resultRow;
                for (const auto &[table, column] : columnMap)
                {
                    if (table == join.table1 && row1.values.find(column) != row1.values.end())
                    {
                        resultRow[table + "." + column] = row1.values.at(column);
                    }
                    else if (table == join.table2 && row2.values.find(column) != row2.values.end())
                    {
                        resultRow[table + "." + column] = row2.values.at(column);
                    }
                }
                if (condition.empty() || evaluateConditionOnJoinRow(resultRow, condition))
                {
                    result.rows.push_back(resultRow);
                }
            }
        }

        // For LEFT JOIN, add rows from left table that had no matches
        if (join.joinType == "LEFT")
        {
            bool hasMatch = false;
            for (const auto &row2 : table2->data)
            {
                if (row1.values.find(join.column1) != row1.values.end() &&
                    row2.values.find(join.column2) != row2.values.end())
                {
                    auto val1 = row1.values.at(join.column1);
                    auto val2 = row2.values.at(join.column2);
                    if (std::any_cast<int>(val1) == std::any_cast<int>(val2))
                    {
                        hasMatch = true;
                        break;
                    }
                }
            }
            if (!hasMatch)
            {
                std::map<std::string, std::any> resultRow;
                for (const auto &[table, column] : columnMap)
                {
                    if (table == join.table1 && row1.values.find(column) != row1.values.end())
                    {
                        resultRow[table + "." + column] = row1.values.at(column);
                    }
                    else if (table == join.table2)
                    {
                        resultRow[table + "." + column] = std::any(); // NULL
                    }
                }
                if (condition.empty() || evaluateConditionOnJoinRow(resultRow, condition))
                {
                    result.rows.push_back(resultRow);
                }
            }
        }
    }

    result.affectedRows = result.rows.size();
    return result;
}

// Helper function to evaluate conditions on joined rows
bool evaluateConditionOnJoinRow(const std::map<std::string, std::any> &row, const std::string &condition)
{
    if (condition.empty())
        return true;

    std::string cond = condition;

    // Parse simple condition
    size_t eqPos = cond.find('=');
    if (eqPos != std::string::npos)
    {
        std::string column = cond.substr(0, eqPos);
        std::string value = cond.substr(eqPos + 1);

        column = trim(column);
        value = trim(value);

        // Remove quotes
        if (value.length() >= 2 && (value.front() == '"' || value.front() == '\''))
        {
            value = value.substr(1, value.length() - 2);
        }

        // Find column in row
        for (const auto &[colName, val] : row)
        {
            if (colName == column || colName.find(column) != std::string::npos)
            {
                if (val.has_value())
                {
                    if (val.type() == typeid(int))
                    {
                        int intVal = std::any_cast<int>(val);
                        int cmpVal = std::stoi(value);
                        return intVal == cmpVal;
                    }
                    else if (val.type() == typeid(std::string))
                    {
                        std::string strVal = std::any_cast<std::string>(val);
                        return strVal == value;
                    }
                    else if (val.type() == typeid(float))
                    {
                        float floatVal = std::any_cast<float>(val);
                        float cmpVal = std::stof(value);
                        return std::abs(floatVal - cmpVal) < 0.0001;
                    }
                }
                break;
            }
        }
    }

    return true;
}

// Helper function for LIKE pattern matching
bool likeMatch(const std::string &str, const std::string &pattern)
{
    std::string regexPattern;
    regexPattern = "^";

    for (char c : pattern)
    {
        if (c == '%')
        {
            regexPattern += ".*";
        }
        else if (c == '_')
        {
            regexPattern += ".";
        }
        else if (c == '.' || c == '?' || c == '*' || c == '+' || c == '^' || c == '$' ||
                 c == '[' || c == ']' || c == '{' || c == '}' || c == '(' || c == ')' ||
                 c == '|' || c == '\\')
        {
            regexPattern += '\\';
            regexPattern += c;
        }
        else
        {
            regexPattern += c;
        }
    }

    regexPattern += "$";

    std::regex regex(regexPattern, std::regex::icase);
    return std::regex_match(str, regex);
}

// Helper function to evaluate conditions on joined rows
bool evaluateCondition(const Row &row, const std::string &condition)
{
    if (condition.empty())
        return true;

    std::string cond = condition;

    // Handle BETWEEN operator
    size_t betweenPos = cond.find(" BETWEEN ");
    if (betweenPos != std::string::npos)
    {
        std::string column = cond.substr(0, betweenPos);
        std::string rest = cond.substr(betweenPos + 9); // 9 = length of " BETWEEN "

        size_t andPos = rest.find(" AND ");
        if (andPos != std::string::npos)
        {
            std::string value1 = rest.substr(0, andPos);
            std::string value2 = rest.substr(andPos + 5);

            column = trim(column);
            value1 = trim(value1);
            value2 = trim(value2);

            // Remove quotes if present
            if (value1.length() >= 2 && (value1.front() == '"' || value1.front() == '\''))
            {
                value1 = value1.substr(1, value1.length() - 2);
            }
            if (value2.length() >= 2 && (value2.front() == '"' || value2.front() == '\''))
            {
                value2 = value2.substr(1, value2.length() - 2);
            }

            if (row.values.find(column) == row.values.end())
            {
                return false;
            }

            auto &val = row.values.at(column);

            if (val.type() == typeid(int))
            {
                int intVal = std::any_cast<int>(val);
                int cmpVal1 = std::stoi(value1);
                int cmpVal2 = std::stoi(value2);
                return intVal >= cmpVal1 && intVal <= cmpVal2;
            }
            else if (val.type() == typeid(float))
            {
                float floatVal = std::any_cast<float>(val);
                float cmpVal1 = std::stof(value1);
                float cmpVal2 = std::stof(value2);
                return floatVal >= cmpVal1 && floatVal <= cmpVal2;
            }
        }
    }

    // Handle NOT BETWEEN operator
    size_t notBetweenPos = cond.find(" NOT BETWEEN ");
    if (notBetweenPos != std::string::npos)
    {
        std::string column = cond.substr(0, notBetweenPos);
        std::string rest = cond.substr(notBetweenPos + 13); // 13 = length of " NOT BETWEEN "

        size_t andPos = rest.find(" AND ");
        if (andPos != std::string::npos)
        {
            std::string value1 = rest.substr(0, andPos);
            std::string value2 = rest.substr(andPos + 5);

            column = trim(column);
            value1 = trim(value1);
            value2 = trim(value2);

            if (value1.length() >= 2 && (value1.front() == '"' || value1.front() == '\''))
            {
                value1 = value1.substr(1, value1.length() - 2);
            }
            if (value2.length() >= 2 && (value2.front() == '"' || value2.front() == '\''))
            {
                value2 = value2.substr(1, value2.length() - 2);
            }

            if (row.values.find(column) == row.values.end())
            {
                return false;
            }

            auto &val = row.values.at(column);

            if (val.type() == typeid(int))
            {
                int intVal = std::any_cast<int>(val);
                int cmpVal1 = std::stoi(value1);
                int cmpVal2 = std::stoi(value2);
                return !(intVal >= cmpVal1 && intVal <= cmpVal2);
            }
            else if (val.type() == typeid(float))
            {
                float floatVal = std::any_cast<float>(val);
                float cmpVal1 = std::stof(value1);
                float cmpVal2 = std::stof(value2);
                return !(floatVal >= cmpVal1 && floatVal <= cmpVal2);
            }
        }
    }

    // Handle NOT operator
    size_t notPos = cond.find(" NOT ");
    if (notPos != std::string::npos)
    {
        // Find the part after NOT
        std::string subCond = cond.substr(notPos + 5); // 5 = length of " NOT "
        return !evaluateCondition(row, subCond);
    }

    // Handle AND conditions
    size_t andPos = cond.find(" AND ");
    if (andPos != std::string::npos)
    {
        std::string leftCond = cond.substr(0, andPos);
        std::string rightCond = cond.substr(andPos + 5);
        return evaluateCondition(row, leftCond) && evaluateCondition(row, rightCond);
    }

    // Handle OR conditions
    size_t orPos = cond.find(" OR ");
    if (orPos != std::string::npos)
    {
        std::string leftCond = cond.substr(0, orPos);
        std::string rightCond = cond.substr(orPos + 4);
        return evaluateCondition(row, leftCond) || evaluateCondition(row, rightCond);
    }

    // FIRST: Check for != operator (two characters)
    size_t nePos = cond.find("!=");
    if (nePos != std::string::npos)
    {
        std::string column = cond.substr(0, nePos);
        std::string value = cond.substr(nePos + 2);

        column = trim(column);
        value = trim(value);

        // Remove quotes
        if (value.length() >= 2 && (value.front() == '"' || value.front() == '\''))
        {
            value = value.substr(1, value.length() - 2);
        }

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(std::string))
        {
            std::string strVal = std::any_cast<std::string>(val);
            return strVal != value;
        }
        else if (val.type() == typeid(int))
        {
            int intVal = std::any_cast<int>(val);
            int cmpVal = std::stoi(value);
            return intVal != cmpVal;
        }
        else if (val.type() == typeid(float))
        {
            float floatVal = std::any_cast<float>(val);
            float cmpVal = std::stof(value);
            return floatVal != cmpVal;
        }
    }

    // Handle NOT LIKE operator
    size_t notLikePos = cond.find(" NOT LIKE ");
    if (notLikePos != std::string::npos)
    {
        std::string column = cond.substr(0, notLikePos);
        std::string pattern = cond.substr(notLikePos + 10); // 10 = length of " NOT LIKE "

        column = trim(column);
        pattern = trim(pattern);

        // Remove quotes from pattern
        if (pattern.length() >= 2 && (pattern.front() == '"' || pattern.front() == '\''))
        {
            pattern = pattern.substr(1, pattern.length() - 2);
        }

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(std::string))
        {
            std::string strVal = std::any_cast<std::string>(val);
            return !likeMatch(strVal, pattern);
        }

        return false;
    }

    // Handle LIKE operator
    size_t likePos = cond.find(" LIKE ");
    if (likePos != std::string::npos)
    {
        std::string column = cond.substr(0, likePos);
        std::string pattern = cond.substr(likePos + 6); // 6 = length of " LIKE "

        column = trim(column);
        pattern = trim(pattern);

        // Remove quotes from pattern
        if (pattern.length() >= 2 && (pattern.front() == '"' || pattern.front() == '\''))
        {
            pattern = pattern.substr(1, pattern.length() - 2);
        }

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(std::string))
        {
            std::string strVal = std::any_cast<std::string>(val);
            return likeMatch(strVal, pattern);
        }

        return false;
    }

    // THEN: Check for >= operator
    size_t gePos = cond.find(">=");
    if (gePos != std::string::npos)
    {
        std::string column = cond.substr(0, gePos);
        std::string value = cond.substr(gePos + 2);

        column = trim(column);
        value = trim(value);

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(int))
        {
            int intVal = std::any_cast<int>(val);
            int cmpVal = std::stoi(value);
            return intVal >= cmpVal;
        }
        else if (val.type() == typeid(float))
        {
            float floatVal = std::any_cast<float>(val);
            float cmpVal = std::stof(value);
            return floatVal >= cmpVal;
        }
    }

    // THEN: Check for <= operator
    size_t lePos = cond.find("<=");
    if (lePos != std::string::npos)
    {
        std::string column = cond.substr(0, lePos);
        std::string value = cond.substr(lePos + 2);

        column = trim(column);
        value = trim(value);

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(int))
        {
            int intVal = std::any_cast<int>(val);
            int cmpVal = std::stoi(value);
            return intVal <= cmpVal;
        }
        else if (val.type() == typeid(float))
        {
            float floatVal = std::any_cast<float>(val);
            float cmpVal = std::stof(value);
            return floatVal <= cmpVal;
        }
    }

    // THEN: Check for = operator
    size_t eqPos = cond.find('=');
    if (eqPos != std::string::npos)
    {
        std::string column = cond.substr(0, eqPos);
        std::string value = cond.substr(eqPos + 1);

        column = trim(column);
        value = trim(value);

        // Remove quotes
        if (value.length() >= 2 && (value.front() == '"' || value.front() == '\''))
        {
            value = value.substr(1, value.length() - 2);
        }

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(std::string))
        {
            std::string strVal = std::any_cast<std::string>(val);
            return strVal == value;
        }
        else if (val.type() == typeid(int))
        {
            int intVal = std::any_cast<int>(val);
            int cmpVal = std::stoi(value);
            return intVal == cmpVal;
        }
        else if (val.type() == typeid(float))
        {
            float floatVal = std::any_cast<float>(val);
            float cmpVal = std::stof(value);
            return floatVal == cmpVal;
        }
    }

    // THEN: Check for > operator
    size_t gtPos = cond.find('>');
    if (gtPos != std::string::npos)
    {
        std::string column = cond.substr(0, gtPos);
        std::string value = cond.substr(gtPos + 1);

        column = trim(column);
        value = trim(value);

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(int))
        {
            int intVal = std::any_cast<int>(val);
            int cmpVal = std::stoi(value);
            return intVal > cmpVal;
        }
        else if (val.type() == typeid(float))
        {
            float floatVal = std::any_cast<float>(val);
            float cmpVal = std::stof(value);
            return floatVal > cmpVal;
        }
    }

    // THEN: Check for < operator
    size_t ltPos = cond.find('<');
    if (ltPos != std::string::npos)
    {
        std::string column = cond.substr(0, ltPos);
        std::string value = cond.substr(ltPos + 1);

        column = trim(column);
        value = trim(value);

        if (row.values.find(column) == row.values.end())
        {
            return false;
        }

        auto &val = row.values.at(column);

        if (val.type() == typeid(int))
        {
            int intVal = std::any_cast<int>(val);
            int cmpVal = std::stoi(value);
            return intVal < cmpVal;
        }
        else if (val.type() == typeid(float))
        {
            float floatVal = std::any_cast<float>(val);
            float cmpVal = std::stof(value);
            return floatVal < cmpVal;
        }
    }

    return false;
}

bool insertInto(const std::string &tableName, const Row &row)
{
    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return false;
    }

    if (transactionActive)
    {
        transactionLog.push_back({"INSERT", tableName});
    }

    bool result = tables[tableName]->insertRow(row);

    // Auto-save after successful insert
    if (result && !currentDatabase.empty() && !transactionActive)
    {
        saveToFile(currentDatabase);
    }

    return result;
}

bool updateRecords(const std::string &tableName, const std::map<std::string, std::any> &setValues, const std::string &condition)
{
    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return false;
    }

    auto &table = tables[tableName];
    int updatedCount = 0;

    for (auto &row : table->data)
    {
        if (condition.empty() || evaluateCondition(row, condition))
        {
            Row updateRow;
            updateRow.values = setValues;
            if (table->updateRow(row.id, updateRow))
            {
                updatedCount++;
            }
        }
    }

    std::cout << updatedCount << " row(s) updated" << std::endl;

    // Auto-save after successful update
    if (updatedCount > 0 && !currentDatabase.empty() && !transactionActive)
    {
        saveToFile(currentDatabase);
    }

    return true;
}

bool deleteRecords(const std::string &tableName, const std::string &condition)
{
    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return false;
    }

    auto &table = tables[tableName];
    int deletedCount = 0;

    for (int i = table->data.size() - 1; i >= 0; i--)
    {
        if (condition.empty() || evaluateCondition(table->data[i], condition))
        {
            if (table->deleteRow(table->data[i].id))
            {
                deletedCount++;
            }
        }
    }

    std::cout << deletedCount << " row(s) deleted" << std::endl;

    // Auto-save after successful delete
    if (deletedCount > 0 && !currentDatabase.empty() && !transactionActive)
    {
        saveToFile(currentDatabase);
    }

    return true;
}

QueryResult select(const std::string &tableName, const std::vector<std::string> &columns, const std::string &condition, const std::string &orderBy, const std::string &orderDir)
{
    QueryResult result;

    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return result;
    }

    auto &table = tables[tableName];

    // Set result columns
    if (columns.size() == 1 && columns[0] == "*")
    {
        for (const auto &col : table->columns)
        {
            result.columns.push_back(col.name);
        }
    }
    else
    {
        result.columns = columns;
    }

    // Select rows with condition
    std::vector<Row> selectedRows;
    for (const auto &row : table->data)
    {
        if (condition.empty() || evaluateCondition(row, condition))
        {
            selectedRows.push_back(row);
        }
    }

    // Apply ordering - Support multiple columns
    if (!orderBy.empty())
    {
        // Parse multiple order by columns (comma separated)
        std::vector<std::pair<std::string, std::string>> orderColumns;
        std::string orderByStr = orderBy;

        // Also parse the full order by with directions if passed
        size_t pos = 0;
        while (pos < orderByStr.length())
        {
            size_t commaPos = orderByStr.find(',', pos);
            std::string colPart = (commaPos == std::string::npos) ? orderByStr.substr(pos) : orderByStr.substr(pos, commaPos - pos);

            // Trim
            size_t first = colPart.find_first_not_of(" \t\n\r");
            size_t last = colPart.find_last_not_of(" \t\n\r");
            if (first != std::string::npos && last != std::string::npos)
            {
                colPart = colPart.substr(first, last - first + 1);
            }

            // Check if column has direction
            size_t spacePos = colPart.find(' ');
            std::string column = colPart;
            std::string direction = "ASC";

            if (spacePos != std::string::npos)
            {
                column = colPart.substr(0, spacePos);
                std::string dir = colPart.substr(spacePos + 1);
                std::transform(dir.begin(), dir.end(), dir.begin(), ::toupper);
                if (dir == "DESC")
                {
                    direction = "DESC";
                }
            }

            orderColumns.push_back({column, direction});
            pos = (commaPos == std::string::npos) ? orderByStr.length() : commaPos + 1;
        }

        // Sort using multiple columns
        std::sort(selectedRows.begin(), selectedRows.end(), [&](const Row &a, const Row &b)
                  {
            for (const auto &orderCol : orderColumns)
            {
                const std::string &col = orderCol.first;
                const std::string &dir = orderCol.second;
                
                auto valA = a.values.find(col);
                auto valB = b.values.find(col);
                
                // Handle missing values
                if (valA == a.values.end() && valB == b.values.end()) continue;
                if (valA == a.values.end()) return dir == "ASC" ? true : false;
                if (valB == b.values.end()) return dir == "ASC" ? false : true;
                
                // Compare based on type
                if (valA->second.type() == typeid(int))
                {
                    int intA = std::any_cast<int>(valA->second);
                    int intB = std::any_cast<int>(valB->second);
                    if (intA != intB)
                    {
                        return dir == "ASC" ? intA < intB : intA > intB;
                    }
                }
                else if (valA->second.type() == typeid(float))
                {
                    float floatA = std::any_cast<float>(valA->second);
                    float floatB = std::any_cast<float>(valB->second);
                    if (floatA != floatB)
                    {
                        return dir == "ASC" ? floatA < floatB : floatA > floatB;
                    }
                }
                else if (valA->second.type() == typeid(std::string))
                {
                    std::string strA = std::any_cast<std::string>(valA->second);
                    std::string strB = std::any_cast<std::string>(valB->second);
                    if (strA != strB)
                    {
                        return dir == "ASC" ? strA < strB : strA > strB;
                    }
                }
                // If equal, continue to next column
            }
            return false; });
    }

    // Build result rows
    for (const auto &row : selectedRows)
    {
        std::map<std::string, std::any> resultRow;
        for (const auto &col : result.columns)
        {
            if (row.values.find(col) != row.values.end())
            {
                resultRow[col] = row.values.at(col);
            }
            else
            {
                resultRow[col] = std::any();
            }
        }
        result.rows.push_back(resultRow);
    }

    result.affectedRows = result.rows.size();
    return result;
}

QueryResult joinTables(const JoinCondition &join, const std::vector<std::string> &columns, const std::string &condition)
{
    QueryResult result;

    if (tables.find(join.table1) == tables.end() || tables.find(join.table2) == tables.end())
    {
        std::cout << "Error: One or both tables not found" << std::endl;
        return result;
    }

    auto &table1 = tables[join.table1];
    auto &table2 = tables[join.table2];

    // Set result columns
    for (const auto &col : columns)
    {
        result.columns.push_back(col);
    }

    // Perform join
    for (const auto &row1 : table1->data)
    {
        for (const auto &row2 : table2->data)
        {
            bool joinCondition = false;

            // Check join condition
            if (row1.values.find(join.column1) != row1.values.end() &&
                row2.values.find(join.column2) != row2.values.end())
            {
                auto val1 = row1.values.at(join.column1);
                auto val2 = row2.values.at(join.column2);

                if (val1.type() == typeid(int) && val2.type() == typeid(int))
                {
                    joinCondition = (std::any_cast<int>(val1) == std::any_cast<int>(val2));
                }
                else if (val1.type() == typeid(std::string) && val2.type() == typeid(std::string))
                {
                    joinCondition = (std::any_cast<std::string>(val1) == std::any_cast<std::string>(val2));
                }
            }

            if (join.joinType == "INNER" && joinCondition)
            {
                std::map<std::string, std::any> resultRow;
                for (const auto &col : columns)
                {
                    size_t dotPos = col.find('.');
                    if (dotPos != std::string::npos)
                    {
                        std::string table = col.substr(0, dotPos);
                        std::string column = col.substr(dotPos + 1);

                        if (table == join.table1 && row1.values.find(column) != row1.values.end())
                        {
                            resultRow[col] = row1.values.at(column);
                        }
                        else if (table == join.table2 && row2.values.find(column) != row2.values.end())
                        {
                            resultRow[col] = row2.values.at(column);
                        }
                    }
                }
                result.rows.push_back(resultRow);
            }
        }
    }

    result.affectedRows = result.rows.size();
    std::cout << "Join completed. " << result.affectedRows << " rows returned" << std::endl;
    return result;
}

QueryResult aggregateFunction(const std::string &function, const std::string &tableName, const std::string &column, const std::string &condition)
{
    QueryResult result;

    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return result;
    }

    auto &table = tables[tableName];
    std::vector<std::any> values;

    // Collect values that match condition
    for (const auto &row : table->data)
    {
        if (condition.empty() || evaluateCondition(row, condition))
        {
            if (row.values.find(column) != row.values.end())
            {
                values.push_back(row.values.at(column));
            }
        }
    }

    if (values.empty())
    {
        std::cout << "No values found" << std::endl;
        return result;
    }

    result.columns.push_back(function + "(" + column + ")");
    std::map<std::string, std::any> resultRow;

    // Apply aggregate function
    std::string funcUpper = function;
    std::transform(funcUpper.begin(), funcUpper.end(), funcUpper.begin(), ::toupper);

    if (funcUpper == "COUNT")
    {
        resultRow[result.columns[0]] = std::any((int)values.size());
    }
    else if (funcUpper == "SUM" || funcUpper == "AVG")
    {
        double sum = 0;
        int count = 0;
        for (const auto &val : values)
        {
            if (val.type() == typeid(int))
            {
                sum += std::any_cast<int>(val);
                count++;
            }
            else if (val.type() == typeid(float))
            {
                sum += std::any_cast<float>(val);
                count++;
            }
        }

        if (funcUpper == "SUM")
        {
            resultRow[result.columns[0]] = std::any(sum);
        }
        else
        {
            resultRow[result.columns[0]] = std::any(sum / count);
        }
    }
    else if (funcUpper == "MAX" || funcUpper == "MIN")
    {
        if (values[0].type() == typeid(int))
        {
            int extreme = std::any_cast<int>(values[0]);
            for (const auto &val : values)
            {
                int intVal = std::any_cast<int>(val);
                if (funcUpper == "MAX")
                    extreme = std::max(extreme, intVal);
                else
                    extreme = std::min(extreme, intVal);
            }
            resultRow[result.columns[0]] = std::any(extreme);
        }
        else if (values[0].type() == typeid(std::string))
        {
            std::string extreme = std::any_cast<std::string>(values[0]);
            for (const auto &val : values)
            {
                std::string strVal = std::any_cast<std::string>(val);
                if (funcUpper == "MAX")
                    extreme = std::max(extreme, strVal);
                else
                    extreme = std::min(extreme, strVal);
            }
            resultRow[result.columns[0]] = std::any(extreme);
        }
    }

    result.rows.push_back(resultRow);
    result.affectedRows = 1;
    return result;
}

bool exportToCSV(const std::string &tableName, const std::string &filename)
{
    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return false;
    }

    auto &table = tables[tableName];
    std::ofstream file(filename);

    if (!file)
    {
        std::cout << "Error: Could not create CSV file" << std::endl;
        return false;
    }

    // Write headers
    for (size_t i = 0; i < table->columns.size(); i++)
    {
        file << table->columns[i].name;
        if (i < table->columns.size() - 1)
            file << ",";
    }
    file << "\n";

    // Write data
    for (const auto &row : table->data)
    {
        for (size_t i = 0; i < table->columns.size(); i++)
        {
            const auto &col = table->columns[i];
            if (row.values.find(col.name) != row.values.end())
            {
                auto &val = row.values.at(col.name);
                if (val.type() == typeid(int))
                {
                    file << std::any_cast<int>(val);
                }
                else if (val.type() == typeid(std::string))
                {
                    file << "\"" << std::any_cast<std::string>(val) << "\"";
                }
                else if (val.type() == typeid(float))
                {
                    file << std::any_cast<float>(val);
                }
            }
            if (i < table->columns.size() - 1)
                file << ",";
        }
        file << "\n";
    }

    file.close();
    std::cout << "Table '" << tableName << "' exported to '" << filename << "' successfully" << std::endl;
    return true;
}

bool backupDatabase(const std::string &backupName)
{
    if (currentDatabase.empty())
    {
        std::cout << "Error: No database selected" << std::endl;
        return false;
    }

    std::string backupPath = "backups/" + backupName;
    fs::create_directories(backupPath);

    std::string dbPath = currentDatabase + "_db";
    if (fs::exists(dbPath))
    {
        fs::copy(dbPath, backupPath, fs::copy_options::recursive);
        std::cout << "Database '" << currentDatabase << "' backed up to '" << backupName << "' successfully" << std::endl;
        return true;
    }

    std::cout << "Error: Could not backup database" << std::endl;
    return false;
}

bool restoreDatabase(const std::string &backupName)
{
    std::string backupPath = "backups/" + backupName;

    if (!fs::exists(backupPath))
    {
        std::cout << "Error: Backup '" << backupName << "' not found" << std::endl;
        return false;
    }

    if (!currentDatabase.empty())
    {
        saveToFile(currentDatabase);
    }

    // Restore from backup
    for (const auto &entry : fs::directory_iterator(backupPath))
    {
        std::string dbName = entry.path().filename().string();
        dbName = dbName.substr(0, dbName.find("_db"));

        std::string targetPath = dbName + "_db";
        fs::remove_all(targetPath);
        fs::copy(entry.path(), targetPath, fs::copy_options::recursive);

        if (std::find(databases.begin(), databases.end(), dbName) == databases.end())
        {
            databases.push_back(dbName);
        }
    }

    saveDatabaseList();
    std::cout << "Database restored from backup '" << backupName << "' successfully" << std::endl;
    return true;
}

bool createIndex(const std::string &tableName, const std::string &columnName)
{
    if (tables.find(tableName) == tables.end())
    {
        std::cout << "Error: Table " << tableName << " does not exist" << std::endl;
        return false;
    }

    tables[tableName]->addIndex(columnName);
    return true;
}

bool beginTransaction()
{
    if (transactionActive)
    {
        std::cout << "Transaction already active" << std::endl;
        return false;
    }

    transactionActive = true;
    transactionLog.clear();
    transactionBackup.clear();

    // Create backup
    for (const auto &[name, table] : tables)
    {
        std::map<int, Row> backup;
        for (const auto &row : table->data)
        {
            backup[row.id] = row;
        }
        transactionBackup[name] = backup;
    }

    std::cout << "Transaction started" << std::endl;
    return true;
}

bool commitTransaction()
{
    if (!transactionActive)
    {
        std::cout << "No active transaction" << std::endl;
        return false;
    }

    transactionActive = false;
    transactionLog.clear();
    transactionBackup.clear();

    std::cout << "Transaction committed" << std::endl;
    return true;
}

bool rollbackTransaction()
{
    if (!transactionActive)
    {
        std::cout << "No active transaction" << std::endl;
        return false;
    }

    // Restore from backup
    for (auto &[name, table] : tables)
    {
        auto &backup = transactionBackup[name];
        table->data.clear();
        for (const auto &[id, row] : backup)
        {
            table->data.push_back(row);
        }
    }

    transactionActive = false;
    transactionLog.clear();
    transactionBackup.clear();

    std::cout << "Transaction rolled back" << std::endl;
    return true;
}

void printTable(const QueryResult &result)
{
    if (result.rows.empty())
    {
        std::cout << "Empty set (0 rows)" << std::endl;
        return;
    }

    // Calculate column widths
    std::map<std::string, int> colWidths;
    for (const auto &col : result.columns)
    {
        colWidths[col] = col.length();
    }

    for (const auto &row : result.rows)
    {
        for (const auto &col : result.columns)
        {
            if (row.find(col) != row.end() && row.at(col).has_value())
            {
                std::string val;
                if (row.at(col).type() == typeid(int))
                {
                    val = std::to_string(std::any_cast<int>(row.at(col)));
                }
                else if (row.at(col).type() == typeid(std::string))
                {
                    val = std::any_cast<std::string>(row.at(col));
                }
                else if (row.at(col).type() == typeid(float))
                {
                    val = std::to_string(std::any_cast<float>(row.at(col)));
                }
                colWidths[col] = std::max(colWidths[col], (int)val.length());
            }
        }
    }

    // Print top border
    std::cout << "\n+";
    for (const auto &col : result.columns)
    {
        std::cout << std::string(colWidths[col] + 2, '-') << "+";
    }
    std::cout << std::endl;

    // Print headers
    std::cout << "|";
    for (const auto &col : result.columns)
    {
        std::cout << " " << std::left << std::setw(colWidths[col]) << col << " |";
    }
    std::cout << std::endl;

    // Print separator
    std::cout << "+";
    for (const auto &col : result.columns)
    {
        std::cout << std::string(colWidths[col] + 2, '-') << "+";
    }
    std::cout << std::endl;

    // Print rows
    for (const auto &row : result.rows)
    {
        std::cout << "|";
        for (const auto &col : result.columns)
        {
            std::string val = "NULL";
            if (row.find(col) != row.end() && row.at(col).has_value())
            {
                if (row.at(col).type() == typeid(int))
                {
                    val = std::to_string(std::any_cast<int>(row.at(col)));
                }
                else if (row.at(col).type() == typeid(std::string))
                {
                    val = std::any_cast<std::string>(row.at(col));
                }
                else if (row.at(col).type() == typeid(float))
                {
                    val = std::to_string(std::any_cast<float>(row.at(col)));
                }
            }
            std::cout << " " << std::left << std::setw(colWidths[col]) << val << " |";
        }
        std::cout << std::endl;
    }

    // Print bottom border
    std::cout << "+";
    for (const auto &col : result.columns)
    {
        std::cout << std::string(colWidths[col] + 2, '-') << "+";
    }
    std::cout << "\n"
              << result.rows.size() << " row(s) in set\n"
              << std::endl;
}
