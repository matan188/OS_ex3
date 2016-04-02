#include "Search.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>

const int MULTI_THREAD_LEVEL = 5;

int main(int argc, char * argv[])
{
    
    for(int i = 0; i < argc; ++i) {
        std::cout << argv[i] << std::endl;
    }
    /*
    Search s;
    
    std::string query = "q";
    
    IN_ITEM in_item1(new Query(query), new Directory("dir1"));
    IN_ITEM in_item2(new Query(query), new Directory("dir2"));
    IN_ITEM in_item3(new Query(query), new Directory("dir3"));
    
    OUT_ITEMS_LIST outItemsList;
    IN_ITEMS_LIST inItemsList = *(new IN_ITEMS_LIST);
    
    inItemsList.push_back(in_item1);
    inItemsList.push_back(in_item2);
    inItemsList.push_back(in_item3);
    
    outItemsList = runMapReduceFramework(dynamic_cast<MapReduceBase&>(s), inItemsList, MULTI_THREAD_LEVEL);
    */
    return 0;
}

bool Query::operator<(const k1Base &other) const {
    return this->getVal() < (dynamic_cast<const Query&>(other)).getVal();
};


void Search::Map(const k1Base *const key, const v1Base *const val) const {
    
}
    
void Search::Reduce(const k2Base *const key, const V2_LIST &vals) const {

}
