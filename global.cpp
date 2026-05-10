#include "global.h"
#include "db.h"  // Add this include for saveToFile and loadFromFile
#include <iostream>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <iomanip>

namespace fs = std::filesystem;

// Forward declarations
void saveToFile(const std::string& database);
void loadFromFile(const std::string& database);

std::map<std::string, std::unique_ptr<Table>> tables;
std::string currentDatabase = "";
bool transactionActive = false;
std::vector<std::pair<std::string, std::string>> transactionLog;
std::map<std::string, std::map<int, Row>> transactionBackup;
std::vector<std::string> databases;

void Table::addIndex(const std::string& columnName) {
    auto index = std::make_unique<BPlusTree<int, std::string>>(4, currentDatabase + "_" + name + "_" + columnName + ".idx");
    
    for (size_t i = 0; i < data.size(); i++) {
        if (data[i].values.find(columnName) != data[i].values.end()) {
            int key = data[i].id;
            std::string value;
            auto& val = data[i].values[columnName];
            if (val.type() == typeid(int)) {
                value = std::to_string(std::any_cast<int>(val));
            } else if (val.type() == typeid(std::string)) {
                value = std::any_cast<std::string>(val);
            } else if (val.type() == typeid(float)) {
                value = std::to_string(std::any_cast<float>(val));
            }
            index->insert(key, value);
        }
    }
    
    indexes[columnName] = std::move(index);
    std::cout << "Index created on column: " << columnName << std::endl;
}

bool Table::insertRow(const Row& row) {
    int newId = getNextId();
    Row newRow(newId);
    newRow.values = row.values;
    
    // Check constraints
    for (const auto& col : columns) {
        // Check NOT NULL
        if (col.notNull && newRow.values.find(col.name) == newRow.values.end()) {
            std::cout << "Error: Column " << col.name << " cannot be NULL" << std::endl;
            return false;
        }
        
        // Check PRIMARY KEY
        if (col.isPrimaryKey && newRow.values.find(col.name) != newRow.values.end()) {
            auto& val = newRow.values[col.name];
            int intVal = std::any_cast<int>(val);
            for (const auto& existingRow : data) {
                if (existingRow.values.find(col.name) != existingRow.values.end()) {
                    int existingVal = std::any_cast<int>(existingRow.values.at(col.name));
                    if (existingVal == intVal) {
                        std::cout << "Error: Primary key " << intVal << " already exists" << std::endl;
                        return false;
                    }
                }
            }
        }
        
        // Check FOREIGN KEY
        if (col.isForeignKey && newRow.values.find(col.name) != newRow.values.end()) {
            auto& tableRef = tables[col.referencesTable];
            if (tableRef) {
                auto& val = newRow.values[col.name];
                int intVal = std::any_cast<int>(val);
                bool found = false;
                for (const auto& rowData : tableRef->data) {
                    if (rowData.values.find(col.referencesColumn) != rowData.values.end()) {
                        int refVal = std::any_cast<int>(rowData.values.at(col.referencesColumn));
                        if (refVal == intVal) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    std::cout << "Error: Foreign key violation - value " << intVal << " not found in " << col.referencesTable << std::endl;
                    return false;
                }
            }
        }
        
        // Handle DEFAULT value
        if (newRow.values.find(col.name) == newRow.values.end() && col.defaultValue.has_value()) {
            newRow.values[col.name] = col.defaultValue;
        }
    }
    
    data.push_back(newRow);
    
    // Update indexes
    for (auto& idx : indexes) {
        int key = newId;
        std::string value;
        if (newRow.values.find(idx.first) != newRow.values.end()) {
            auto& val = newRow.values[idx.first];
            if (val.type() == typeid(int)) {
                value = std::to_string(std::any_cast<int>(val));
            } else if (val.type() == typeid(std::string)) {
                value = std::any_cast<std::string>(val);
            }
            idx.second->insert(key, value);
        }
    }
    
    std::cout << "Row inserted successfully (ID: " << newId << ")" << std::endl;
    return true;
}

bool Table::updateRow(int id, const Row& row) {
    for (size_t i = 0; i < data.size(); i++) {
        if (data[i].id == id) {
            for (const auto& [key, value] : row.values) {
                data[i].values[key] = value;
                
                // Update indexes
                if (indexes.find(key) != indexes.end()) {
                    std::string strValue;
                    if (value.type() == typeid(int)) {
                        strValue = std::to_string(std::any_cast<int>(value));
                    } else if (value.type() == typeid(std::string)) {
                        strValue = std::any_cast<std::string>(value);
                    }
                    indexes[key]->update(id, strValue);
                }
            }
            std::cout << "Row updated successfully" << std::endl;
            return true;
        }
    }
    std::cout << "Error: Row with ID " << id << " not found" << std::endl;
    return false;
}

bool Table::deleteRow(int id) {
    for (size_t i = 0; i < data.size(); i++) {
        if (data[i].id == id) {
            // Remove from indexes
            for (auto& idx : indexes) {
                idx.second->remove(id);
            }
            
            data.erase(data.begin() + i);
            std::cout << "Row deleted successfully" << std::endl;
            return true;
        }
    }
    std::cout << "Error: Row with ID " << id << " not found" << std::endl;
    return false;
}

Row* Table::getRow(int id) {
    for (auto& row : data) {
        if (row.id == id) {
            return &row;
        }
    }
    return nullptr;
}

std::vector<Row> Table::search(const std::string& condition) {
    std::vector<Row> results;
    
    for (const auto& row : data) {
        if (evaluateCondition(row, condition)) {
            results.push_back(row);
        }
    }
    
    return results;
}

// Database management functions implementation
bool createDatabase(const std::string& dbName) {
    if (std::find(databases.begin(), databases.end(), dbName) != databases.end()) {
        std::cout << "Error: Database '" << dbName << "' already exists!" << std::endl;
        return false;
    }
    
    std::string dbPath = dbName + "_db";
    if (!fs::create_directory(dbPath)) {
        std::cout << "Error: Could not create database directory!" << std::endl;
        return false;
    }
    
    databases.push_back(dbName);
    saveDatabaseList();
    
    std::cout << "Database '" << dbName << "' created successfully!" << std::endl;
    return true;
}

bool dropDatabase(const std::string& dbName) {
    auto it = std::find(databases.begin(), databases.end(), dbName);
    if (it == databases.end()) {
        std::cout << "Error: Database '" << dbName << "' does not exist!" << std::endl;
        return false;
    }
    
    if (currentDatabase == dbName) {
        tables.clear();
        currentDatabase = "";
    }
    
    std::string dbPath = dbName + "_db";
    fs::remove_all(dbPath);
    
    databases.erase(it);
    saveDatabaseList();
    
    std::cout << "Database '" << dbName << "' dropped successfully!" << std::endl;
    return true;
}

bool useDatabase(const std::string& dbName) {
    if (std::find(databases.begin(), databases.end(), dbName) == databases.end()) {
        std::cout << "Error: Database '" << dbName << "' does not exist!" << std::endl;
        std::cout << "Create it first using: CREATE DATABASE " << dbName << ";" << std::endl;
        return false;
    }
    
    if (!currentDatabase.empty()) {
        saveToFile(currentDatabase);
    }
    
    currentDatabase = dbName;
    loadFromFile(dbName);
    
    std::cout << "Using database: " << dbName << std::endl;
    return true;
}

void listDatabases() {
    if (databases.empty()) {
        std::cout << "No databases found." << std::endl;
        return;
    }
    
    std::cout << "\n+------------------+" << std::endl;
    std::cout << "| Databases        |" << std::endl;
    std::cout << "+------------------+" << std::endl;
    for (const auto& db : databases) {
        std::cout << "| " << std::left << std::setw(16) << db;
        if (db == currentDatabase) {
            std::cout << " (current)";
        }
        std::cout << " |" << std::endl;
    }
    std::cout << "+------------------+\n" << std::endl;
}

void showTables() {
    if (tables.empty()) {
        std::cout << "No tables found in database '" << currentDatabase << "'" << std::endl;
        return;
    }
    
    std::cout << "\n+------------------+" << std::endl;
    std::cout << "| Tables           |" << std::endl;
    std::cout << "+------------------+" << std::endl;
    for (const auto& [name, table] : tables) {
        std::cout << "| " << std::left << std::setw(16) << name << " |" << std::endl;
    }
    std::cout << "+------------------+\n" << std::endl;
}

void describeTable(const std::string& tableName) {
    auto it = tables.find(tableName);
    if (it == tables.end()) {
        std::cout << "Error: Table '" << tableName << "' does not exist" << std::endl;
        return;
    }
    
    std::cout << "\n+--------------+--------------+--------+----------+" << std::endl;
    std::cout << "| Column       | Type         | Null   | Key      |" << std::endl;
    std::cout << "+--------------+--------------+--------+----------+" << std::endl;
    
    for (const auto& col : it->second->columns) {
        std::string typeStr;
        switch(col.type) {
            case DataType::INT: typeStr = "INT"; break;
            case DataType::STRING: typeStr = "VARCHAR(" + std::to_string(col.length) + ")"; break;
            case DataType::FLOAT: typeStr = "FLOAT"; break;
            case DataType::BOOL: typeStr = "BOOL"; break;
            case DataType::DATE: typeStr = "DATE"; break;
        }
        
        std::string nullStr = col.notNull ? "NO" : "YES";
        std::string keyStr = "";
        if (col.isPrimaryKey) keyStr = "PRI";
        if (col.isForeignKey) keyStr = "FOREIGN";
        
        std::cout << "| " << std::left << std::setw(12) << col.name 
                  << " | " << std::setw(12) << typeStr
                  << " | " << std::setw(6) << nullStr
                  << " | " << std::setw(8) << keyStr << " |" << std::endl;
    }
    std::cout << "+--------------+--------------+--------+----------+\n" << std::endl;
}

void saveDatabaseList() {
    std::ofstream file("databases.list");
    for (const auto& db : databases) {
        file << db << std::endl;
    }
}

void loadDatabaseList() {
    std::ifstream file("databases.list");
    if (!file) return;
    
    std::string dbName;
    while (std::getline(file, dbName)) {
        if (!dbName.empty()) {
            databases.push_back(dbName);
        }
    }
}

void saveToFile(const std::string& database) {
    if (database.empty()) return;
    
    std::string dbPath = database + "_db";
    if (!fs::exists(dbPath)) {
        fs::create_directory(dbPath);
    }
    
    std::ofstream file(dbPath + "/schema.txt");
    for (const auto& [name, table] : tables) {
        file << "TABLE:" << name << "\n";
        
        // Save column definitions
        for (const auto& col : table->columns) {
            file << "COLUMN:" << col.name << ":" << (int)col.type << ":" << col.length << ":" 
                 << col.notNull << ":" << col.isPrimaryKey << ":" << col.isForeignKey << "\n";
        }
        
        // Save data
        for (const auto& row : table->data) {
            file << "ROW:" << row.id;
            for (const auto& [colName, val] : row.values) {
                if (val.type() == typeid(int)) {
                    file << "|" << colName << "=I:" << std::any_cast<int>(val);
                } else if (val.type() == typeid(std::string)) {
                    file << "|" << colName << "=S:" << std::any_cast<std::string>(val);
                } else if (val.type() == typeid(float)) {
                    file << "|" << colName << "=F:" << std::any_cast<float>(val);
                }
            }
            file << "\n";
        }
        file << "END\n";
    }
}

void loadFromFile(const std::string& database) {
    if (database.empty()) return;
    
    std::string dbPath = database + "_db/schema.txt";
    std::ifstream file(dbPath);
    if (!file) return;
    
    tables.clear();
    
    std::string line;
    std::unique_ptr<Table> currentTable = nullptr;
    
    while (std::getline(file, line)) {
        if (line.substr(0, 6) == "TABLE:") {
            if (currentTable) {
                tables[currentTable->name] = std::move(currentTable);
            }
            std::string tableName = line.substr(6);
            currentTable = std::make_unique<Table>(tableName);
        }
        else if (line.substr(0, 7) == "COLUMN:") {
            std::stringstream ss(line.substr(7));
            std::string colName, typeStr, lenStr, notNullStr, pkStr, fkStr;
            std::getline(ss, colName, ':');
            std::getline(ss, typeStr, ':');
            std::getline(ss, lenStr, ':');
            std::getline(ss, notNullStr, ':');
            std::getline(ss, pkStr, ':');
            std::getline(ss, fkStr, ':');
            
            Column col;
            col.name = colName;
            col.type = (DataType)std::stoi(typeStr);
            col.length = std::stoi(lenStr);
            col.notNull = (notNullStr == "1");
            col.isPrimaryKey = (pkStr == "1");
            col.isForeignKey = (fkStr == "1");
            
            if (currentTable) {
                currentTable->columns.push_back(col);
            }
        }
        else if (line.substr(0, 4) == "ROW:") {
            if (!currentTable) continue;
            
            std::string rowData = line.substr(4);
            size_t pipePos = rowData.find('|');
            std::string idStr = rowData.substr(0, pipePos);
            int id = std::stoi(idStr);
            
            Row row(id);
            currentTable->nextId = std::max(currentTable->nextId, id + 1);
            
            std::string remaining = rowData.substr(pipePos + 1);
            size_t start = 0;
            while (start < remaining.length()) {
                size_t nextPipe = remaining.find('|', start);
                if (nextPipe == std::string::npos) nextPipe = remaining.length();
                
                std::string field = remaining.substr(start, nextPipe - start);
                size_t eqPos = field.find('=');
                if (eqPos != std::string::npos) {
                    std::string colName = field.substr(0, eqPos);
                    std::string valuePart = field.substr(eqPos + 1);
                    char typeChar = valuePart[0];
                    std::string actualValue = valuePart.substr(2);
                    
                    if (typeChar == 'I') {
                        row.values[colName] = std::any(std::stoi(actualValue));
                    } else if (typeChar == 'S') {
                        row.values[colName] = std::any(actualValue);
                    } else if (typeChar == 'F') {
                        row.values[colName] = std::any(std::stof(actualValue));
                    }
                }
                
                start = nextPipe + 1;
            }
            
            currentTable->data.push_back(row);
        }
        else if (line == "END") {
            if (currentTable) {
                tables[currentTable->name] = std::move(currentTable);
                currentTable = nullptr;
            }
        }
    }
}