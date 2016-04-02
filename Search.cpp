#include "Search.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>

const int MULTI_THREAD_LEVEL = 5;

int main(int argc, char * argv[])
{
    
    Query * q1 = new Query("5");
    Query * q2 = new Query("2");
    
    if(*q1 < *q2) {
        std::cout << "q1 is smaller than q2" << std::endl;
    } else {
        std::cout << "q1 is bigger than q2" << std::endl;
    }
    
    Search s;
    
    outItemList = runMapReduceFramework(s, inItemList, MULTI_THREAD_LEVEL);

    
    return 0;
}

bool Query::operator<(const k1Base &other) const {
    return this->getVal() < (dynamic_cast<const Query&>(other)).getVal();
};