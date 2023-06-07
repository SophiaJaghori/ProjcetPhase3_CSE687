#ifdef CMAPPERLIBRARY_EXPORTS
#define CMAPPERLIBRARY_API __declspec(dllexport)
#else
#define CMAPPERLIBRARY_API __declspec(dllimport)
#endif
#define WIN32_LEAN_AND_MEAN
#pragma once

#include <string>
#include "../ConsoleApplication19/CFileManager.h"
#include "boost/algorithm/string/regex.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp> 
#include <boost/range/algorithm/count.hpp>
#include <map>
#include "MapperInterface.h"

class CMapper :public ICMapper
{
    private:
        struct WordNode {
            std::string Word;
            int count;
        };
        std::multimap<std::string, int, std::less<std::string>> wordMap;
    public:
        /*The map class will contain a public method map(), that accepts a key and value.
          The key will be the file name and the value will be a single line of raw data from the file*/
        std::string Map(std::string filename, std::string strline);
        void Map(std::string filename, std::string strline, std::string partition);
        /*
        The map() function will tokenize the value into distinct words (remove punctuation, whitespace and capitalization).
        */
        std::string tokenize_line(std::string strLine);
        /*
        * The export function will buffer output in memory and periodically write the data out to disk (periodicity can be based on the size of the buffer).
          The intermediate data will be written to the temporary directory (specified via command line).
        */
        std::multimap<std::string, int, std::less<std::string>> exportData();
        void bufferFlush();
        double Add(double a, double b);
};

//extern "C" CMAPPERLIBRARY_API int Mapper(std::string a, std::string b);
//extern "C" CMAPPERLIBRARY_API ICMap * _cdecl CreateMapObject();
//  Factory function that will return the new object instance. (Only function
//  should be declared with DLLCALL)
extern "C" /*Important for avoiding Name decoration*/
{
    CMAPPERLIBRARY_API ICMapper* _cdecl CreateMapperObject();
};

