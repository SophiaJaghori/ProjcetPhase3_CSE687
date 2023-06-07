#ifdef CREDUCERLIBRARY_EXPORTS
#define CREDUCERLIBRARY_API __declspec(dllexport)
#else
#define CREDUCERLIBRARY_API __declspec(dllimport)
#endif
#define WIN32_LEAN_AND_MEAN
#pragma once

#include "boost/algorithm/string/regex.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp> 
#include <boost/range/algorithm/count.hpp>
#include "ReducerInterface.h"

class CReducer :public ICReducer
{
    std::map<std::string, std::string> wordCountMap;
    std::map<std::string, int> wordCountReducer;
    std::string filenamePath;
    std::string reducerFileName = "reducer";
public:
    double Add(double, double);
    void clear();
    void do_accumulate_job(std::string lines);
    void Reduce(std::map<std::string, std::string> wordAccumCount);
    /* write the result key and resulting reduced value  to the output directory*/
    std::map<std::string,int> exportData();
    void bufferFlush();
    std::string getReducerFileName();
        
};
extern "C" /*Important for avoiding Name decoration*/
{
    CREDUCERLIBRARY_API ICReducer* _cdecl CreateReducerObject();
};
