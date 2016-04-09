#include "MapReduceFramework.h"
#include <pthread.h>

IN_ITEMS_LIST inContainer;

void * mapExec(void * p) {
    // read chenk
    // call func
    std::cout << "Thread Job" << std::endl;
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    pthread_exit(NULL);
}

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
                                     IN_ITEMS_LIST& itemsList,
                                     int multiThreadLevel) {
    OUT_ITEMS_LIST outItemsList;
    inContainer = itemsList;
    std::list<pthread_t*> threads;
    
    //(void*) map (const k1Base *const, const v1Base *const) = &MapReduceBase::Map;
    //mapExec(mapReduce);
    
    for(int i = 0; i < multiThreadLevel; ++i) {
        // create ExecMap thread
        pthread_t p;
        pthread_create(&p, NULL, mapExec, &mapReduce);
        threads.push_back(&p);
    }
    
    
    return outItemsList;
}


void Emit2 (k2Base*, v2Base*) {
    
}

void Emit3 (k3Base*, v3Base*) {
    
}