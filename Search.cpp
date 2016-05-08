#include "Search.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <dirent.h>
#include <unistd.h>
#include <regex>

const int MULTI_THREAD_LEVEL = 5;

typedef std::pair<k2Base*, v2Base*> MID_ITEM;
typedef std::vector<MID_ITEM> MID_ITEMS_LIST;
MID_ITEMS_LIST vecMid;

int getFilesInDir(std::string dir, std::list<std::string> &files) {
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dir.c_str())) == NULL) {
        std::cout << "Error(" << errno << ") opening " << dir << std::endl;
        return errno;
    }
    
    while ((dirp = readdir(dp)) != NULL) {
        files.push_back(std::string(dirp->d_name));
    }
    closedir(dp);
    return 0;
}

int main(int argc, char * argv[])
{
    
    // parse arguments
    if(argc < 2) {
        std::cout << "Arguments missing" << std::endl;
        exit(1);
    }
    std::string query = argv[1];
    std::list<std::string> directories;
    for(int i = 2; i < argc; ++i) {
        directories.push_back(argv[i]);
    }
    
    // init search
    Search s;
    
    OUT_ITEMS_LIST outItemsList;
    IN_ITEMS_LIST inItemsList;
    for(std::string dir : directories) {
        IN_ITEM item(new Query(query), new Directory(dir));
        inItemsList.push_back(item);
    }
    
    
    outItemsList = runMapReduceFramework(dynamic_cast<MapReduceBase&>(s),
                                        inItemsList, MULTI_THREAD_LEVEL);


    for(auto it = outItemsList.begin(); it != outItemsList.end(); ++it) {
        for(int i = 0; i < (static_cast<Counter *>(it->second))->getVal(); ++i) {
            std::cout << (static_cast<FileName2 *>(it->first)->getVal()) <<
            std::endl;
        }
    }

    for(auto it : inItemsList) {
        delete it.first;
        delete it.second;
    }


    for(auto it : outItemsList) {
        delete it.first;
        delete it.second;
    }

    for(auto it : vecMid) {
        delete it.first;
        delete it.second;
    }

    return 0;
}

//TODO remove prints
bool Query::operator<(const k1Base &other) const {
    return this->getVal() < (dynamic_cast<const Query&>(other)).getVal();
};

bool FileName1::operator<(const k2Base &other) const {
    return this->getVal() < (dynamic_cast<const FileName1&>(other)).getVal();
};

bool FileName2::operator<(const k3Base &other) const {
    return this->getVal() < (dynamic_cast<const FileName2&>(other)).getVal();
};

Search::Search() {
}


void Search::Map(const k1Base *const key, const v1Base *const val) const {
    std::list<std::string> files;
    std::string query = dynamic_cast<const Query&>(*key).getVal();
    std::string dir = static_cast<const Directory&>(*val).getVal();
    getFilesInDir(dir, files);
    
    std::regex regexPattern(".*" + query + ".*");
    for(std::string str : files) {
        if(std::regex_match(str, regexPattern)) {
            /*typedef std::pair<k2Base*, v2Base*> MID_ITEM;
            typedef std::list<MID_ITEM> MID_ITEMS_LIST;*/
            MID_ITEM item = {new FileName1(str), new Weight(1)};
            Emit2(item.first, item.second);
            vecMid.push_back(item);
        }
    }
}

void Search::Reduce(const k2Base *const key, const V2_LIST &vals) const {
    int sum = 0;
    for(v2Base * val : vals) {
        sum += static_cast<const Weight&>(*val).getVal();
    }
    std::string file = static_cast<const FileName1&>(*key).getVal();
    Emit3(new FileName2(file), new Counter(sum));
}