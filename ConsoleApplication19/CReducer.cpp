#include "CReducer.h"

void CReducer::clear() {
    wordCountMap.clear();
    wordCountReducer.clear();
}
//void CReducer::do_accumulate_job(std::string lines)
//{
//    // convert all words to lowercase to avoid duplicate counts. 
//    const std::string lower_sline = boost::algorithm::to_lower_copy(lines);
//    std::vector<std::string> words;
//
//    // split into words array with delimiter '-'. So ACT - 1 would be stored as word[0]=ACT and word[1]=1 
//    boost::split(words, lower_sline, boost::is_any_of("-"));
//
//    // check if words exist add 1 to accumualte else add word to list.
//    if (wordCountMap.find(words[0]) == wordCountMap.end())
//        // add word and count.
//        wordCountMap.insert(std::pair<std::string, std::string>(words[0], words[1]));
//    else
//        wordCountMap[words[0]] = wordCountMap[words[0]] + "1";
//}

void CReducer::Reduce(std::string strline)
{
    std::size_t count;
    std::map<std::string, std::string>::iterator itrMap;

    std::cout << std::endl << "Output from Accumulator files." << std::endl;
    // printing words and occurances side by side. Example ACT 1111
    std::map< std::string, std::string>::iterator itr;
 //   for (itr = wordCountAccum.begin(); itr != wordCountAccum.end(); ++itr) {
 //      std::cout << '\t' << itr->first << '\t' << itr->second << std::endl;
  //  }

    //for (itrMap = wordCountAccum.begin(); itrMap != wordCountAccum.end(); ++itrMap) {
    //    count = boost::count(itrMap->second, '1');
    //    if (count > 1)
    //    {
    //      
    //        //// check if given word is already present in the map
    //        //if (wordCountReducer.find(itrMap->first) == wordCountReducer.end())
    //        //{
    //        //    // If word is not present then add word with one occurance
    //        //    wordCountReducer.insert(std::pair< std::string, int>(itrMap->first, count));
    //        //}
    //        //else
    //        //{
    //        //    // If word is present increment count of occurance.
    //        //  wordCountReducer[itrMap->first] += count;
    //        //}
    //    }
    //    else
    //        wordCountReducer.insert(std::pair<std::string, int>(itrMap->first, count));
    //}
    //std::cout << std::endl << "Output from Reducer job." << std::endl;

    //std::map< std::string, int>::iterator itr1;
    //for (itr1 = wordCountReducer.begin(); itr1 != wordCountReducer.end(); ++itr1) {
    //    std::cout << '\t' << itr1->first << '\t' << itr1->second << std::endl;
    //}
}

/* write the result key and resulting reduced value  to the output directory    */
void CReducer::exportData(std::string outputDir) {

    std::string filenamePath = outputDir + "\\" + reducerFileName + ".txt";
    CFileManager FManager;

    std::multimap<std::string, int>::iterator itr;
    std::string strline;

    std::string strWord;
    std::string strCount;

    for (itr = wordCountReducer.begin(); itr != wordCountReducer.end(); ++itr) {
        strWord = itr->first;
        strCount = std::to_string(itr->second);
        strline = strWord + " - " + strCount;
        FManager.writeOutputFile(filenamePath, strline);
    }
    // clear buffer 
    wordCountReducer.clear();
};
    
/*
Upon success an additional empty file SUCCESS will be written out to the output directory.
The reduce method itself should not directly deal with any File IO.
*/
    void CReducer::writeEmptySuccessfile(std::string outputDir) {
        CFileManager FManager;
        std::string filenamePath = outputDir + "\\" + "SUCCESS.txt";
        FManager.writeOutputFile(filenamePath, "SUCCESS");
    };

