#include "MapReduceFramework.h"
#include "Search.h"
#include <pthread.h>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <map>
#include <algorithm>
#include <vector>
#include <unistd.h>
#include <ctime>
#include <sys/time.h>
#include <iomanip>
#include <sstream>

#define FILE_LOCATION ".MapReduceFramework.log"
#define CHUNK_SIZE 20

pthread_mutex_t logMut;
pthread_mutex_t listIndexMut;
pthread_mutex_t mapInitMut;

pthread_cond_t cond;

int listIndex = 0;
int postShuffleContainerIndex = 0;
int activeThreads;

IN_ITEMS_LIST inContainer;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*>
                                                        mapContainers;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*>
                                                        mapBufferedContainers;
std::map<pthread_t, pthread_mutex_t> mapContainersMut;
std::map<pthread_t, std::vector<std::pair<k3Base*, v3Base*>*>*>
                                                        reduceContainers;

std::ofstream logFile;
std::stringstream logBuffer;

void sysError(std::string errFunc) {
    std::cerr << "MapReduceFramework Failure: " <<
        errFunc << " failed." << std::endl;
    exit(1);
}

void init() {
    logMut = PTHREAD_MUTEX_INITIALIZER;
    listIndexMut = PTHREAD_MUTEX_INITIALIZER;
    mapInitMut = PTHREAD_MUTEX_INITIALIZER;
    cond = PTHREAD_COND_INITIALIZER;
}

int logCounter = 0;

void writeLogToDisk() {
    logFile.open(FILE_LOCATION, std::ios_base::app);
    if(logFile.fail()) {
        sysError("open");
    }
    std::string s = logBuffer.str();
    logFile << s;
    logFile.close();
    if(logFile.fail()) {
        sysError("close");
    }
    logBuffer.str("");
    logCounter = 0;
}

void writeToLog(std::string msg) {
    logBuffer << msg;
    if(logCounter == 100) {
        writeLogToDisk();
    }
}


struct cmpK2Base {
    bool operator()(const k2Base * a, const k2Base * b) const {
        return  *a < *b;
    }
};

std::map<k2Base *, V2_LIST, cmpK2Base> postShuffleContainer;

bool cmpK3Base (std::pair<k3Base*, v3Base*> a, std::pair<k3Base*, v3Base*> b) {
    return  *(a.first) < *(b.first);
}


const double S_TO_NANO = 1000000000;
const double M_TO_NANO = 1000;

/**
 * Final calculations to return the times measures
 */
double timesCalc(double sec1, double micro1, double sec2, double micro2)
{
    /* Converting the seconds and microseconds to nanoseconds */
    double secInNano = (sec2 - sec1) * S_TO_NANO;
    double microInNano = (micro2 - micro1) * M_TO_NANO;

    return (double) (secInNano + microInNano);
};

/**
 * Global variables clean up.
 */
void cleanUp() {
    for(auto it : mapContainers) {
        delete it.second;
    }

    for(auto it : mapBufferedContainers) {
        delete it.second;
    }

    for(auto it : reduceContainers) {
        for(auto innerIt : *(it.second)) {
            delete  innerIt;
        }
        delete it.second;
    }
    listIndex = 0;
    mapContainers.clear();
    postShuffleContainer.clear();
    mapBufferedContainers.clear();
    reduceContainers.clear();
    inContainer.clear();
    mapContainersMut.clear();
    postShuffleContainerIndex = 0;
    activeThreads = 0;
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&mapInitMut);
    pthread_mutex_destroy(&logMut);
    pthread_mutex_destroy(&listIndexMut);
    logBuffer.str("");
}

/**
 * Return current time and date for logs
 */
std::string  returnTime(){
    std::time_t rawtime;
    std::tm* timeinfo;
    char buffer [80];

    std::time(&rawtime);
    timeinfo = std::localtime(&rawtime);

    std::strftime(buffer,80,"%d.%m.%Y %H:%M:%S",timeinfo);

    return buffer;
};

void clearBuffer() {
    auto selfId = pthread_self();
    pthread_mutex_lock(&mapContainersMut[selfId]);
    auto m = mapBufferedContainers[selfId];
    for(auto it = m->begin(); it != m->end(); ++it) {
        mapContainers[selfId]->push_back(*it);
    }

    m->clear();
    pthread_mutex_unlock(&mapContainersMut[selfId]);
    pthread_cond_signal(&cond);
}

void * mapExec(void * p) {

    pthread_mutex_lock(&mapInitMut);
    pthread_mutex_unlock(&mapInitMut);

    pthread_mutex_lock(&logMut);
    writeToLog("Thread ExecMap created [" + returnTime() + "]\n");
    pthread_mutex_unlock(&logMut);

    
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;

    
    while(true) {
        if(listIndex >= (int) inContainer.size()) {
            clearBuffer();

            pthread_mutex_lock(&logMut);
            writeToLog("Thread ExecMap terminated [" + returnTime() + "]\n");
            pthread_mutex_unlock(&logMut);

            pthread_exit(NULL);
        }
        pthread_mutex_lock(&listIndexMut);
        currentChunk = listIndex;
        listIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&listIndexMut);
        
        auto it = inContainer.begin();
        std::advance(it, currentChunk);
        for(int i = currentChunk; i < (int) inContainer.size()
                                  && i < currentChunk + CHUNK_SIZE; ++i) {
            auto query = it->first;
            auto dir = it->second;
            mapReduce->Map(query, dir);
            ++it;
        }
        if(mapBufferedContainers[pthread_self()]->size() >= CHUNK_SIZE) {
            clearBuffer();
        }
    }



}

void * reduceExec(void * p) {

    pthread_mutex_lock(&logMut);
    writeToLog("Thread ExecReduce created [" + returnTime() + "]\n");
    pthread_mutex_unlock(&logMut);

    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k3Base*, v3Base*>*> * container;
    try {
         container = new std::vector<std::pair<k3Base*, v3Base*>*>();
    } catch(...) {
        sysError("new");
    }
    
    pthread_mutex_lock(&mapInitMut);
    reduceContainers[pthread_self()] = container;
    pthread_mutex_unlock(&mapInitMut);

    while(true) {
        if(postShuffleContainerIndex >= (int) postShuffleContainer.size()) {

            pthread_mutex_lock(&logMut);
            writeToLog("Thread ExecReduce terminated [" + returnTime()
                       + "]\n");
            pthread_mutex_unlock(&logMut);

            pthread_exit(NULL);
        }
        pthread_mutex_lock(&listIndexMut);
        currentChunk = postShuffleContainerIndex;
        postShuffleContainerIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&listIndexMut);

        auto it = postShuffleContainer.begin();
        std::advance(it, currentChunk);
        for(int i = currentChunk; i < (int) postShuffleContainer.size()
                                  && i < currentChunk + CHUNK_SIZE; ++i) {
            k2Base * filename2 = it->first;
            std::list<v2Base *> counter = it->second;
            mapReduce->Reduce(filename2, counter);
            ++it;
        }
    }

}

void * shuffle(void *) {

    pthread_mutex_lock(&mapInitMut);
    pthread_mutex_unlock(&mapInitMut);

    pthread_mutex_lock(&logMut);
    writeToLog("Thread Shuffle created [" + returnTime() + "]\n");
    pthread_mutex_unlock(&logMut);

    timespec ts;
    pthread_mutex_t tempMut = PTHREAD_MUTEX_INITIALIZER;
    while(1) {

        // search for non empty list
        for(auto it = mapContainers.begin(); it != mapContainers.end(); ++it) {
            pthread_t key = it->first;
            auto list = it->second;

            if(!list->empty()) {
                pthread_mutex_lock(&mapContainersMut[key]);
                for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                    k2Base* k2 = (*it2)->first;
                    v2Base* v2 = (*it2)->second;
                    if(postShuffleContainer.count(k2)){
                        postShuffleContainer[k2].push_back(v2);
                    } else {
                        postShuffleContainer[k2] = {v2};
                    }
                }
                list->clear();
                pthread_mutex_unlock(&mapContainersMut[key]);
            }
        }
        if(activeThreads == 0) {
            activeThreads--;
        } else if(activeThreads == -1) {
            break;
        }
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000; // wait only 0.01 sec at most
        pthread_cond_timedwait(&cond, &tempMut, &ts);
    }

    pthread_exit(NULL);
}

OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce,
                                     IN_ITEMS_LIST& itemsList,
                                     int multiThreadLevel) {
    init();
    
    int t_res;
    
    pthread_mutex_lock(&logMut);
    writeToLog("runMapReduceFramework started with " +
                       std::to_string(multiThreadLevel) + " threads\n");
    pthread_mutex_unlock(&logMut);

    timeval tim;
    gettimeofday(&tim, NULL);
    double sec1 = tim.tv_sec;
    double micro1 = tim.tv_usec;

    OUT_ITEMS_LIST outItemsList;
    inContainer = itemsList;

    pthread_mutex_lock(&mapInitMut);

    /***********/
    /* SHUFFLE */
    /***********/
    pthread_t shuffleThread;
    t_res = pthread_create(&shuffleThread, NULL, shuffle, nullptr);
    if(t_res != 0) {
        sysError("pthread_create");
    }
    
    /*******/
    /* MAP */
    /*******/
    pthread_t * threadsArray;
    try {
        threadsArray = new pthread_t[multiThreadLevel]();
    } catch(...) {
        sysError("new");
    }
    

    activeThreads = multiThreadLevel;

    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&(threadsArray[i]), NULL, mapExec, &mapReduce);

        std::vector<std::pair<k2Base*, v2Base*>*> * container;
        std::vector<std::pair<k2Base*, v2Base*>*> * bufferedContainer;
        try {
            container = new std::vector<std::pair<k2Base*, v2Base*>*>();
            bufferedContainer = new std::vector<std::pair<k2Base*, v2Base*>*>();
        } catch(...) {
            sysError("new");
        }
        mapContainers[threadsArray[i]] = container;
        mapBufferedContainers[threadsArray[i]] = bufferedContainer;
        mapContainersMut[threadsArray[i]] = PTHREAD_MUTEX_INITIALIZER;
    }
    pthread_mutex_unlock(&mapInitMut);

    for(int i = 0; i < multiThreadLevel; ++i) {
        t_res = pthread_join(threadsArray[i], NULL);
        if(t_res != 0) {
            sysError("pthread_join");
        }
    }


    /**** Join SHUFFLE ****/
    activeThreads = 0;
    pthread_join(shuffleThread, NULL);


    pthread_mutex_lock(&logMut);
    writeToLog("Thread Shuffle terminated [" + returnTime() + "]\n");
    pthread_mutex_unlock(&logMut);


    /*** calculate time for log ***/
    t_res = gettimeofday(&tim, NULL);
    if(t_res != 0) {
        sysError("gettimeofday");
    }
    double sec2 = tim.tv_sec;
    double micro2 = tim.tv_usec;

    long int nanoSeconds = (long int) timesCalc(sec1, micro1, sec2, micro2);

    
    /**********/
    /* REDUCE */
    /**********/

    timeval reduceTim;
    t_res = gettimeofday(&reduceTim, NULL);
    if(t_res != 0) {
        sysError("gettimeofday");
    }
    double reduceSec1 = reduceTim.tv_sec;
    double reduceMicro1 = reduceTim.tv_usec;

    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&(threadsArray[i]), NULL, reduceExec, &mapReduce);
    }
    // join to main thread
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threadsArray[i], NULL);
    }

    /************/
    /* FINALIZE */
    /************/

    for(auto it = reduceContainers.begin(); it != reduceContainers.end();
                                                                    ++it) {
        auto list = it->second;
        if(!list->empty()) {
            for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                auto k3 = (*it2)->first;
                auto v3 = (*it2)->second;
                outItemsList.push_back({k3, v3});
            }
            list->clear();
        }
    }

    outItemsList.sort(cmpK3Base);

    t_res = gettimeofday(&tim, NULL);
    if(t_res != 0) {
        sysError("gettimeofday");
    }
    double reduceSec2 = tim.tv_sec;
    double reduceMicro2 = tim.tv_usec;

    long int reduceNanoSeconds = (long int)
            timesCalc(reduceSec1, reduceMicro1, reduceSec2, reduceMicro2);

    /* write to log */
    pthread_mutex_lock(&logMut);
    writeToLog("Map and Shuffle took " + std::to_string(nanoSeconds)
               + " ns\n");
    writeToLog("Reduce took " + std::to_string(reduceNanoSeconds) + " ns\n");
    writeToLog("runMapReduceFramework finished\n");

    writeLogToDisk();

    pthread_mutex_unlock(&logMut);

    /* clean up variables */
    cleanUp();
    delete[] threadsArray;

    return outItemsList;
}


void Emit2 (k2Base* k2, v2Base* v2) {
    std::pair<k2Base*, v2Base*> * p;
    try {
        p = new std::pair<k2Base*, v2Base*>(k2, v2);
    } catch(...) {
        sysError("new");
    }
    mapBufferedContainers[pthread_self()]->push_back(p);
}

void Emit3 (k3Base* k3, v3Base* v3) {
    std::pair<k3Base*, v3Base*> * p;
    try {
        p = new std::pair<k3Base*, v3Base*>(k3, v3);
    } catch(...) {
        sysError("new");
    }
    reduceContainers[pthread_self()]->push_back(p);
}