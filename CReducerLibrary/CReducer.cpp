#include "pch.h"
#include "framework.h"
#include <iostream>
#include "CReducer.h"

// Create Object
CREDUCERLIBRARY_API ICReducer* _cdecl CreateReducerObject() {
    return new CReducer();
};

double CReducer::Add(double a, double b) {
    std::cout << "Inside Mapper (filename,strline)";
    return a + b;
};

void CReducer::clear() {
    wordCountMap.clear();
    wordCountReducer.clear();
};

void CReducer::do_accumulate_job(std::string lines) {
    // convert all words to lowercase to avoid duplicate counts. 
    const std::string lower_sline = boost::algorithm::to_lower_copy(lines);
    std::vector<std::string> words;

    // split into words array with delimiter '-'. So ACT - 1 would be stored as word[0]=ACT and word[1]=1 
    boost::split(words, lower_sline, boost::is_any_of("-"));

    // check if words exist add 1 to accumualte else add word to list.
    if (wordCountMap.find(words[0]) == wordCountMap.end())
        // add word and count.
        wordCountMap.insert(std::pair<std::string, std::string>(words[0], words[1]));
    else
        wordCountMap[words[0]] = wordCountMap[words[0]] + "1";
};

void CReducer::Reduce(std::map<std::string, std::string> wordAccumCount) {
    int count;
    std::map<std::string, std::string>::iterator itrMap;

    std::cout << std::endl << "Output from Accumulator files." << std::endl;
    // printing words and occurances side by side. Example ACT 1111
    std::map< std::string, std::string>::iterator itr;
    for (itr = wordAccumCount.begin(); itr != wordAccumCount.end(); ++itr) {
        std::cout << '\t' << itr->first << '\t' << itr->second << std::endl;
    }

    for (itrMap = wordAccumCount.begin(); itrMap != wordAccumCount.end(); ++itrMap) {
        count = boost::count(itrMap->second, '1');
        if (count > 1)
        {
            // check if given word is already present in the map
            if (wordCountReducer.find(itrMap->first) == wordCountReducer.end())
            {
                // If word is not present then add word with one occurance
                wordCountReducer.insert(std::pair< std::string, int>(itrMap->first, count));
            }
            else
            {
                // If word is present increment count of occurance.
                wordCountReducer[itrMap->first] += count;
            }
        }
        else
            wordCountReducer.insert(std::pair<std::string, int>(itrMap->first, count));
    }
    std::cout << std::endl << "Output from Reducer job." << std::endl;

    std::map< std::string, int>::iterator itr1;
    for (itr1 = wordCountReducer.begin(); itr1 != wordCountReducer.end(); ++itr1) {
        std::cout << '\t' << itr1->first << '\t' << itr1->second << std::endl;
    }
};
std::map<std::string, int> CReducer::exportData() {
    // return data to be written to workflow.
    return wordCountReducer;
};

void CReducer::bufferFlush() {
    // clear buffer 
    wordCountReducer.clear();
};

std::string CReducer::getReducerFileName() {
    return reducerFileName;
}
