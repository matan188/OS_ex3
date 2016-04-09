#ifndef Search_h
#define Search_h

#include <stdio.h>
#include <string>
#include "MapReduceClient.h"
#include "MapReduceFramework.h"

class Query : public k1Base {
    std::string _query;
public:
    Query(std::string query) : _query(query) {};
    std::string getVal() const { return _query; };
    bool operator<(const k1Base &other) const override;
};

class Directory : public v1Base {
    std::string _directory;
public:
    Directory(std::string directory) : _directory(directory) {};
    std::string getVal() const { return _directory; };
};

class FileName1 : public k2Base {
    std::string _fileName;
public:
    FileName1(std::string fileName) : _fileName(fileName) {};
    std::string getVal() const { return _fileName; };
    bool operator<(const k2Base &other) const override;
};

class Weight : public v2Base {
    int _weight;
public:
    Weight(int weight) : _weight(weight) {};
    int getVal() const { return _weight; };
};

class FileName2 : public k3Base {
    std::string _fileName;
public:
    FileName2(std::string fileName) : _fileName(fileName) {};
    std::string getVal() const { return _fileName; };
    bool operator<(const k3Base &other) const override;
};

class Counter : public v3Base {
    int _counter;
public:
    Counter(int counter) : _counter(counter) {};
    int getVal() const { return _counter; };
};

typedef std::pair<k2Base*, v2Base*> MID_ITEM;
typedef std::list<MID_ITEM> MID_ITEMS_LIST;

class Search: public MapReduceBase {
    MID_ITEMS_LIST * _mapOutList;
public:
    Search();
    void Map(const k1Base *const key, const v1Base *const val) const override;
    void Reduce(const k2Base *const key, const V2_LIST &vals) const override;
};

#endif /* Search_h */