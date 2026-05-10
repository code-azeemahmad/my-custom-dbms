#include "BPlusTree.h"
#include <sstream>
#include <cctype>

template<typename K, typename V>
BPlusTree<K, V>::BPlusTree(int ord, const std::string& file) : order(ord), filename(file) {
    root = new Node(true);
    if (!filename.empty()) {
        loadFromFile();
    }
}

template<typename K, typename V>
BPlusTree<K, V>::~BPlusTree() {
    writeToFile();
    delete root;
}

template<typename K, typename V>
void BPlusTree<K, V>::insert(const K& key, const V& value) {
    cache[key] = value;
    
    Node* leaf = findLeaf(key);
    
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int pos = it - leaf->keys.begin();
    
    leaf->keys.insert(leaf->keys.begin() + pos, key);
    leaf->values.insert(leaf->values.begin() + pos, value);
    
    if (leaf->keys.size() > order) {
        if (leaf == root) {
            Node* newRoot = new Node(false);
            newRoot->children.push_back(root);
            splitChild(newRoot, 0);
            root = newRoot;
        }
    }
}

template<typename K, typename V>
typename BPlusTree<K, V>::Node* BPlusTree<K, V>::findLeaf(const K& key) {
    Node* current = root;
    while (!current->isLeaf) {
        size_t i = 0;
        while (i < current->keys.size() && key >= current->keys[i]) {
            i++;
        }
        current = current->children[i];
    }
    return current;
}

template<typename K, typename V>
void BPlusTree<K, V>::splitChild(Node* parent, int index) {
    Node* child = parent->children[index];
    Node* newChild = new Node(child->isLeaf);
    
    int mid = child->keys.size() / 2;
    
    for (size_t i = mid; i < child->keys.size(); i++) {
        newChild->keys.push_back(child->keys[i]);
        if (child->isLeaf) {
            newChild->values.push_back(child->values[i]);
        } else {
            newChild->children.push_back(child->children[i]);
        }
    }
    
    child->keys.resize(mid);
    if (child->isLeaf) {
        child->values.resize(mid);
    } else {
        child->children.resize(mid + 1);
    }
    
    parent->keys.insert(parent->keys.begin() + index, newChild->keys[0]);
    parent->children.insert(parent->children.begin() + index + 1, newChild);
    
    if (child->isLeaf) {
        newChild->next = child->next;
        child->next = newChild;
    }
}

template<typename K, typename V>
bool BPlusTree<K, V>::search(const K& key, V& value) {
    if (cache.find(key) != cache.end()) {
        value = cache[key];
        return true;
    }
    
    Node* leaf = findLeaf(key);
    for (size_t i = 0; i < leaf->keys.size(); i++) {
        if (leaf->keys[i] == key) {
            value = leaf->values[i];
            return true;
        }
    }
    return false;
}

template<typename K, typename V>
void BPlusTree<K, V>::update(const K& key, const V& value) {
    insert(key, value);
}

template<typename K, typename V>
bool BPlusTree<K, V>::remove(const K& key) {
    cache.erase(key);
    return true;
}

template<typename K, typename V>
std::vector<std::pair<K, V>> BPlusTree<K, V>::rangeSearch(const K& start, const K& end) {
    std::vector<std::pair<K, V>> results;
    Node* leaf = findLeaf(start);
    
    while (leaf) {
        for (size_t i = 0; i < leaf->keys.size(); i++) {
            if (leaf->keys[i] >= start && leaf->keys[i] <= end) {
                results.push_back({leaf->keys[i], leaf->values[i]});
            }
        }
        leaf = leaf->next;
    }
    return results;
}

template<typename K, typename V>
std::vector<V> BPlusTree<K, V>::getAllValues() {
    std::vector<V> values;
    for (const auto& pair : cache) {
        values.push_back(pair.second);
    }
    return values;
}

template<typename K, typename V>
void BPlusTree<K, V>::writeToFile() {
    if (filename.empty()) return;
    std::ofstream file(filename);
    for (const auto& pair : cache) {
        file << pair.first << "|" << pair.second << "\n";
    }
}

template<typename K, typename V>
void BPlusTree<K, V>::loadFromFile() {
    if (filename.empty()) return;
    std::ifstream file(filename);
    if (!file) return;
    
    K key;
    V value;
    std::string line;
    while (std::getline(file, line)) {
        size_t pos = line.find('|');
        if (pos != std::string::npos) {
            std::string keyStr = line.substr(0, pos);
            std::string valStr = line.substr(pos + 1);
            std::stringstream ss(keyStr);
            ss >> key;
            cache[key] = value;
        }
    }
}

template<typename K, typename V>
void BPlusTree<K, V>::print() {
    printNode(root, 0);
}

template<typename K, typename V>
void BPlusTree<K, V>::printNode(Node* node, int level) {
    if (!node) return;
    
    for (int i = 0; i < level; i++) std::cout << "  ";
    std::cout << "[";
    for (size_t i = 0; i < node->keys.size(); i++) {
        std::cout << node->keys[i];
        if (i < node->keys.size() - 1) std::cout << ",";
    }
    std::cout << "]\n";
    
    for (Node* child : node->children) {
        printNode(child, level + 1);
    }
}

// Explicit template instantiation
template class BPlusTree<int, std::string>;
template class BPlusTree<std::string, std::string>;