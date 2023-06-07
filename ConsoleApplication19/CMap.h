#include "CFileManager.h"
#include "boost/algorithm/string/regex.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp> 
#include <boost/range/algorithm/count.hpp>
#include <map>

#ifndef MAP_H
#define MAP_H
class CMap
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
    void map(std::string filename, std::string strline);
    /*
    The map() function will tokenize the value into distinct words (remove punctuation, whitespace and capitalization).
    */
    void tokenize_line(std::string strLine); 
    /*
    * The export function will buffer output in memory and periodically write the data out to disk (periodicity can be based on the size of the buffer).
      The intermediate data will be written to the temporary directory (specified via command line).
    */
    void exportData(std::string tempDir, std::string filename); 
};

#endif
