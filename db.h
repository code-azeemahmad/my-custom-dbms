#ifndef DB_H
#define DB_H

#include "global.h"
#include <string>
#include <vector>
#include <functional>

// Database management
bool createDatabase(const std::string &dbName);
bool dropDatabase(const std::string &dbName);
bool useDatabase(const std::string &dbName);
void listDatabases();
void showTables();
void describeTable(const std::string &tableName);

// Table operations
bool createTable(const std::string &name, const std::vector<Column> &columns);
bool insertInto(const std::string &tableName, const Row &row);
bool updateRecords(const std::string &tableName, const std::map<std::string, std::any> &setValues, const std::string &condition);
bool deleteRecords(const std::string &tableName, const std::string &condition);

QueryResult select(const std::string &tableName, const std::vector<std::string> &columns, const std::string &condition, const std::string &orderBy, const std::string &orderDir, int limit = 0);

bool createIndex(const std::string &tableName, const std::string &columnName);

// Advanced features
QueryResult aggregateFunction(const std::string &function, const std::string &tableName, const std::string &column, const std::string &condition);
bool exportToCSV(const std::string &tableName, const std::string &filename);
bool backupDatabase(const std::string &backupName);
bool restoreDatabase(const std::string &backupName);

// Transaction functions
bool beginTransaction();
bool commitTransaction();
bool rollbackTransaction();

// File operations
void saveToFile(const std::string &database);
void loadFromFile(const std::string &database);

// Helper functions
void printTable(const QueryResult &result);
bool evaluateCondition(const Row &row, const std::string &condition);
std::any convertValue(const std::string &value, DataType type);

QueryResult executeJoin(const JoinCondition &join, const std::vector<std::string> &columns, const std::string &condition);
bool evaluateConditionOnJoinRow(const std::map<std::string, std::any> &row, const std::string &condition);

#endif