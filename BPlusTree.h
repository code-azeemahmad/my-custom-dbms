#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <vector>
#include <map>
#include <fstream>
#include <string>
#include <iostream>
#include <algorithm>

template<typename K, typename V>
class BPlusTree {
private:
    struct Node {
        bool isLeaf;
        std::vector<K> keys;
        std::vector<V> values;
        std::vector<Node*> children;
        Node* next;
        
        Node(bool leaf = true) : isLeaf(leaf), next(nullptr) {}
        
        ~Node() {
            for (Node* child : children) {
                delete child;
            }
        }
    };
    
    int order;
    Node* root;
    std::string filename;
    std::map<K, V> cache;
    
    void splitChild(Node* parent, int index);
    void insertInternal(Node* node, const K& key, Node* rightChild);
    Node* findLeaf(const K& key);
    void writeToFile();
    void loadFromFile();
    void printNode(Node* node, int level);
    
public:
    BPlusTree(int ord = 4, const std::string& file = "");
    ~BPlusTree();
    
    void insert(const K& key, const V& value);
    bool search(const K& key, V& value);
    bool remove(const K& key);
    void update(const K& key, const V& value);
    std::vector<std::pair<K, V>> rangeSearch(const K& start, const K& end);
    std::vector<V> getAllValues();
    void print();
};

#endif