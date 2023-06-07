#include "CMap.h"

void CMap::map(std::string filename, std::string strline) {
    tokenize_line(strline);
}

/*
The map() function will tokenize the value into distinct words (remove punctuation, whitespace and capitalization).
*/
void CMap::tokenize_line(std::string strLine) {
    int bufferSize = 10;
    std::string templine = boost::erase_all_regex_copy(strLine, boost::regex("[^a-zA-Z0-9 ]+"));
    std::vector<std::string> words;

    // split to tokens line seperated by space and store in vector.
    char space_char = ' ';
    boost::split(words, templine, boost::is_any_of(" "));

    for (int i = 0; i < words.size(); i++)
    {
        if (words[i].length() == 0)
            continue;
        wordMap.insert(std::pair<std::string, int>(words[i], 1));
    }
}
/*
* The export function will buffer output in memory and periodically write the data out to disk (periodicity can be based on the size of the buffer).
  The intermediate data will be written to the temporary directory (specified via command line).
*/
void CMap::exportData(std::string tempDir, std::string filename) {
    std::string filenamePath = tempDir + "\\" + filename + ".txt";

    CFileManager FManager;
    std::multimap<std::string, int>::iterator itr;
    std::string strline;

    std::string strWord;
    std::string strCount;

    for (itr = wordMap.begin(); itr != wordMap.end(); ++itr) {
        strWord = itr->first;
        strCount = std::to_string(itr->second);
        strline = strWord + " - " + strCount;
      //  FManager.writeOutputFile(filenamePath, strline);
    }
    // clear buffer 
    wordMap.clear();
}
