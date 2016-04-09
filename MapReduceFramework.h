#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"
#include <utility>

typedef std::pair<k1Base*, v1Base*> IN_ITEM;
typedef std::pair<k3Base*, v3Base*> OUT_ITEM;

typedef std::list<IN_ITEM> IN_ITEMS_LIST;
typedef std::list<OUT_ITEM> OUT_ITEMS_LIST;

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_LIST& itemsList, int multiThreadLevel);

/*This function is called by the Map function (implemented by the user) in order to add a new pair of
 <k2,v2> to the framework's internal data structures. This function is part of the MapReduceFramework's API.
 See pthread_self() function */
void Emit2 (k2Base*, v2Base*);
void Emit3 (k3Base*, v3Base*);

#endif //MAPREDUCEFRAMEWORK_H
