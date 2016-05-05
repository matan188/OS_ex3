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

//TODO execMap still print after join
//TODO change file location
#define FILE_LOCATION "/cs/stud/matanmo/safe/OS/ex3/logFile.txt"
std::ofstream logFile;

#define CHUNK_SIZE 20

IN_ITEMS_LIST inContainer;

pthread_mutex_t debugMut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t logMut = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t listIndexMut = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mapInitMut = PTHREAD_MUTEX_INITIALIZER;
int listIndex = 0;
int postShuffleContainerIndex = 0;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*> mapContainers;
std::map<pthread_t, std::vector<std::pair<k2Base*, v2Base*>*>*> mapBufferedContainers;
std::map<pthread_t, pthread_mutex_t> mapContainersMut;
std::map<pthread_t, std::vector<std::pair<k3Base*, v3Base*>*>*> reduceContainers;

int activeThreads;

std::stringstream logBuffer;

void writeToLog(std::string msg) {
    logBuffer << msg;
}

struct cmpK2Base {
    bool operator()(const k2Base * a, const k2Base * b) const {
        return  *a < *b;
    }
};
std::map<k2Base *, V2_LIST, cmpK2Base> postShuffleContainer;

bool cmpK3Base (std::pair<k3Base*, v3Base*> a, std::pair<k3Base*, v3Base*> b) {
    return  a.first < b.first;
}

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

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
    
    /* write to log */
    pthread_mutex_lock(&logMut);
    writeToLog("Thread ExecMap created [" + returnTime() + "]\n");
    //std::cout << "Thread ExecMap created [" + returnTime() + "]\n" << std::endl;
    pthread_mutex_unlock(&logMut);

    
    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k2Base*, v2Base*>*> * container = new std::vector<std::pair<k2Base*, v2Base*>*>();
    std::vector<std::pair<k2Base*, v2Base*>*> * bufferedContainer = new std::vector<std::pair<k2Base*, v2Base*>*>();

    
    pthread_mutex_lock(&mapInitMut);
    mapContainers[pthread_self()] = container;
    mapBufferedContainers[pthread_self()] = bufferedContainer;
    mapContainersMut[pthread_self()] = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_unlock(&mapInitMut);

    
    while(true) {
        if(listIndex >= inContainer.size()) {
            clearBuffer();

            /* write to log */
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
        for(int i = currentChunk; i < inContainer.size() && i < currentChunk + CHUNK_SIZE; ++i) {
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

    /* write to log */
    pthread_mutex_lock(&logMut);
    writeToLog("Thread ExecReduce created [" + returnTime() + "]\n");
    pthread_mutex_unlock(&logMut);

    MapReduceBase * mapReduce = (MapReduceBase*)(p);
    int currentChunk;
    std::vector<std::pair<k3Base*, v3Base*>*> * container = new std::vector<std::pair<k3Base*, v3Base*>*>();

    pthread_mutex_lock(&mapInitMut);
    reduceContainers[pthread_self()] = container;
    pthread_mutex_unlock(&mapInitMut);

    while(true) {
        if(postShuffleContainerIndex >= postShuffleContainer.size()) {

            /* write to log */
            pthread_mutex_lock(&logMut);
            writeToLog("Thread ExecReduce terminated [" + returnTime() + "]\n");
            pthread_mutex_unlock(&logMut);

            pthread_exit(NULL);
        }
        pthread_mutex_lock(&listIndexMut);
        currentChunk = postShuffleContainerIndex;
        postShuffleContainerIndex += CHUNK_SIZE;
        pthread_mutex_unlock(&listIndexMut);

        auto it = postShuffleContainer.begin();
        std::advance(it, currentChunk);
        for(int i = currentChunk; i < postShuffleContainer.size() && i < currentChunk + CHUNK_SIZE; ++i) {
            auto filename2 = it->first;
            auto counter = it->second;
            mapReduce->Reduce(filename2, counter);
            ++it;
        }
    }

}

void * shuffle(void * p) {

    /* write to log */
    pthread_mutex_lock(&logMut);
    //std::cout << "Thread Shuffle created [" + returnTime() + "]\n" << std::endl;
    pthread_mutex_unlock(&logMut);

    timespec ts;
    pthread_mutex_t tempMut = PTHREAD_MUTEX_INITIALIZER;
    while(1) {

        // search for non empty list
        for(auto it = mapContainers.begin(); it != mapContainers.end(); ++it) {

            pthread_t key = it->first;
            auto list = it->second;

            if(!list->empty()) {
                // shuffle list
                pthread_mutex_lock(&mapContainersMut[key]);
                for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                    auto k2 = (*it2)->first;
                    auto v2 = (*it2)->second;
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
            //break;
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

    /* write to log */
    pthread_mutex_lock(&logMut);
    writeToLog("runMapReduceFramework started with " + std::to_string(multiThreadLevel) + " threads\n");
    pthread_mutex_unlock(&logMut);

    timeval tim;
    gettimeofday(&tim, NULL);
    double sec1 = tim.tv_sec;
    double micro1 = tim.tv_usec;

    OUT_ITEMS_LIST outItemsList;
    inContainer = itemsList;
    std::list<pthread_t*> threads;

    /***********/
    /* SHUFFLE */
    /***********/
    pthread_t shuffleThread;
    pthread_create(&shuffleThread, NULL, shuffle, nullptr);
    
    /*******/
    /* MAP */
    /*******/
    pthread_t * threadsArray = new pthread_t[multiThreadLevel]();

    
    activeThreads = multiThreadLevel;
    // Create threads
    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(&(threadsArray[i]), NULL, mapExec, &mapReduce);
    }

    for(int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threadsArray[i], NULL);
    }


    /**** Join SHUFFLE ****/
    activeThreads = 0;
    pthread_join(shuffleThread, NULL);

    
    /* write to log */
    pthread_mutex_lock(&logMut);
    writeToLog("Thread Shuffle terminated [" + returnTime() + "]\n");
    pthread_mutex_unlock(&logMut);

    // destroy cond
    pthread_cond_destroy(&cond);


    /*** calculate time for log ***/
    gettimeofday(&tim, NULL);
    double sec2 = tim.tv_sec;
    double micro2 = tim.tv_usec;

    long int nanoSeconds = (long int) timesCalc(sec1, micro1, sec2, micro2);


    std::cout << "before reduce" << std::endl;
    
    /**********/
    /* REDUCE */
    /**********/

    timeval reduceTim;
    gettimeofday(&reduceTim, NULL);
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
    delete[] threadsArray;

    /************/
    /* FINALIZE */
    /************/

    for(auto it = reduceContainers.begin(); it != reduceContainers.end(); ++it) {
        auto list = it->second;
        if(!list->empty()) {
            // shuffle list
            for(auto it2 = list->begin(); it2 != list->end(); ++it2) {
                auto k3 = (*it2)->first;
                auto v3 = (*it2)->second;
                outItemsList.push_back({k3, v3});
            }
            list->clear();
        }
    }

    //////////
    //std::cout << postShuffleContainer.size() << std::endl;

    outItemsList.sort(cmpK3Base);


    gettimeofday(&tim, NULL);
    double reduceSec2 = tim.tv_sec;
    double reduceMicro2 = tim.tv_usec;

    long int reduceNanoSeconds = (long int) timesCalc(reduceSec1, reduceMicro1, reduceSec2, reduceMicro2);


//    sleep(1); //sleep is for checking that the prints are correct
    /* write to log */
    pthread_mutex_lock(&logMut);
    writeToLog("Map and Shuffle took " + std::to_string(nanoSeconds) + " ns\n");
    writeToLog("Reduce took " + std::to_string(reduceNanoSeconds) + " ns\n");
    writeToLog("runMapReduceFramework finished\n");
    pthread_mutex_unlock(&logMut);

    /***/
    /*char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL)
        std::cout << "Current working dir: " << cwd << std::endl;
    else
        std::cout << "getcwd() error" << std::endl;
    */
    /***/
    
    logFile.open(FILE_LOCATION, std::ios_base::app);
    std::string s = logBuffer.str();
    logFile << s;
    logFile.close();

    return outItemsList;
}

void Emit2 (k2Base* k2, v2Base* v2) {
    auto p = new std::pair<k2Base*, v2Base*>(k2, v2);
    mapBufferedContainers[pthread_self()]->push_back(p);
}

void Emit3 (k3Base* k3, v3Base* v3) {
    auto p = new std::pair<k3Base*, v3Base*>(k3, v3);
    reduceContainers[pthread_self()]->push_back(p);
}