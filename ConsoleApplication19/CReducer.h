#include "CFileManager.h"
#include <map>
#ifndef REDUCER_H
#define REDUCER_H

#include "boost/algorithm/string/regex.hpp"
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp> 
#include <boost/range/algorithm/count.hpp>

class CReducer
{
    std::map<std::string, std::string> wordCountMap;
    std::map<std::string, int> wordCountReducer;
    std::string filenamePath;
    std::string reducerFileName = "reducer";
public:
    void clear();
 //   void do_accumulate_job(std::string lines);
    void Reduce(std::string strline);

    /* write the result key and resulting reduced value  to the output directory    */
    void exportData(std::string outputDir);
    /*
    Upon success an additional empty file SUCCESS will be written out to the output directory.
    The reduce method itself should not directly deal with any File IO.
    */
    void writeEmptySuccessfile(std::string outputDir);
};
#endif
