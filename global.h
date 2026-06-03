#ifndef GLOBAL_H
#define GLOBAL_H

#include <string>
#include <vector>
#include <map>
#include <any>
#include <memory>
#include "BPlusTree.h"

enum class DataType {
    INT,
    STRING,
    FLOAT,
    BOOL,
    DATE
};

struct Column {
    std::string name;
    DataType type;
    int length;
    bool notNull;
    bool isPrimaryKey;
    bool isForeignKey;
    bool isUnique;
    bool autoIncrement;
    std::string referencesTable;
    std::string referencesColumn;
    std::any defaultValue;
};

struct Row {
    int id;
    std::map<std::string, std::any> values;
    
    Row() : id(-1) {}
    Row(int rowId) : id(rowId) {}
};

struct JoinCondition {
    std::string table1;
    std::string column1;
    std::string table2;
    std::string column2;
    std::string joinType; // INNER, LEFT, RIGHT
};

struct QueryResult {
    std::vector<std::string> columns;
    std::vector<std::map<std::string, std::any>> rows;
    int affectedRows;
    
    QueryResult() : affectedRows(0) {}
};

class Table {
public:
    std::string name;
    std::vector<Column> columns;
    std::map<std::string, std::unique_ptr<BPlusTree<int, std::string>>> indexes;
    std::vector<Row> data;
    std::map<int, Row> primaryKeyIndex;
    int nextId;
    
    Table() : nextId(1) {}
    Table(const std::string& n) : name(n), nextId(1) {}
    
    void addIndex(const std::string& columnName);
    bool insertRow(const Row& row);
    bool updateRow(int id, const Row& row);
    bool deleteRow(int id);
    Row* getRow(int id);
    std::vector<Row> search(const std::string& condition);
    int getNextId() { return nextId++; }
};

extern std::map<std::string, std::unique_ptr<Table>> tables;
extern std::string currentDatabase;
extern bool transactionActive;
extern std::vector<std::pair<std::string, std::string>> transactionLog;
extern std::map<std::string, std::map<int, Row>> transactionBackup;
extern std::vector<std::string> databases;

// Database management functions
bool createDatabase(const std::string& dbName);
bool dropDatabase(const std::string& dbName);
bool useDatabase(const std::string& dbName);
void listDatabases();
void saveDatabaseList();
void loadDatabaseList();
void showTables();
void describeTable(const std::string& tableName);

#endif