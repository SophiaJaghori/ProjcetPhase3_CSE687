#include <iostream>
#include "CFileManager.h"
#include "CMap.h"
#include "CReducer.h"
#include <windows.h>
#include "MapperInterface.h"
#include "ReducerInterface.h"
#include <thread>
#include <mutex>

using std::thread;
static std::mutex mtx;
CFileManager FileManager;
std::multimap<int, std::string> MergedlinesInFile;

typedef ICMapper* (*CREATE_MAPPER_OBJ)();
typedef ICReducer* (*CREATE_REDUCER_OBJ)();

std::multimap<int, std::string> filesInBatch;
std::vector<std::thread> map_threads;

std::map<std::string, std::string> wordCountMap;
std::map<std::string, int> wordCountReducer;

ICMapper* pMapper1;

std::condition_variable cv;
std::condition_variable cv_reducer;
std::mutex cv_m; // This mutex is used to synchronize accesses for mapper processing
std::mutex cv_r; // This mutex is used to synchronize accesses for reducer processing

// Output files for reducer. 
std::vector<std::string> listOfFilesTemp;

//void runMapper(std::string tempfilename,int nPartition)
void runMapper(int nPartition)
{
// runMapper 
std::multimap<int, std::string>::iterator itr;
std::string strline;
std::string strFileName;
std::string strPartition;
std::string tempfilename = "PART0000_";

int static cond = 0;
static int x = nPartition;
//wi = th;
std::unique_lock<std::mutex> lk(cv_m);
cv.wait(lk, [nPartition] {return cond != nPartition; });

/* Mapper uses Map function from CMapperLibrary DLL.  */
HINSTANCE hDLLMapper;
const wchar_t* libNameMapper = L"CMapperLibrary";

hDLLMapper = LoadLibraryEx(libNameMapper, NULL, NULL); // Handle to DLL
if (hDLLMapper != NULL) {
    CREATE_MAPPER_OBJ  MapperPtr = (CREATE_MAPPER_OBJ)GetProcAddress(hDLLMapper, "CreateMapperObject");
    ICMapper* pMapper = MapperPtr();

    if (pMapper != NULL) {
        for (itr = filesInBatch.begin(); (itr != filesInBatch.end()); ++itr)
        {
            //if (itr->first != R) continue;
            if (itr->first != nPartition) continue;

            std::vector<std::string> linesInFile1;
            strPartition = std::to_string(itr->first);

            strFileName = itr->second;
            std::cout << std::endl;
            std::cout << std::endl << "Running Thread - " << strPartition << " Processing File - " << strFileName << std::endl;
            linesInFile1.clear();
            linesInFile1 = FileManager.readLinesInFile(strFileName);

            for (std::size_t j = 0; j < linesInFile1.size(); ++j)
            {
                std::vector<std::string> words;

                // Call to mapper to tokenize line.
                char space_char = ' ';
                std::string strTokenWord = linesInFile1[j];
                std::string strTokenWord1 = pMapper->Map(strFileName, strTokenWord);

                // Call Mapper DLL to get tokenized words. 
                boost::split(words, pMapper->Map(strFileName, strTokenWord), boost::is_any_of(" "));

                /* Map outputs a separate temporary file that holds (word, 1) for each occurrence of every word.  */
                std::string bucket = std::to_string(nPartition);
                std::string tmpfile;
                tmpfile = tempfilename + bucket;
                /*
                  The export function will buffer output in memory and periodically write the data out to disk
                */
                if (words.size() > 0)
                    FileManager.writeTempOutputFile(tmpfile, words);
                words.clear();
            }
        }
    }
    else
        std::cout << "Did not load mapper library correctly." << std::endl;
    FreeLibrary(hDLLMapper);
    }
    else {
        std::cout << "Mapper Library load failed!" << std::endl;
    }
        cond = nPartition;
        cv.notify_one();
};


void loadPartitions(int nPartition)
{
    int static rcond = 1;
    static int rx = nPartition;
    std::unique_lock<std::mutex> lk(cv_r);
    cv_reducer.wait(lk, [nPartition] {return rcond != nPartition; });

    std::string filename = listOfFilesTemp[nPartition-1];  // Thread 1 but file index will start from 0.
    std::cout << filename << std::endl;
    std::vector<std::string> linesInFile;
    linesInFile = FileManager.readLinesInFile(filename);
    /* Lines in file (linesInFile) will now be like below when loading
    *
        ACT - 1
        BERTRAM - 1
        BERTRAM - 1
        COUNTESS - 1
        COUNTESS - 1
    */
    for (std::size_t j = 0; j < linesInFile.size(); ++j)
        MergedlinesInFile.insert(std::pair<int, std::string>(nPartition, linesInFile[j]));
    // condition is set to processed by thread. 
    rcond = nPartition;
    cv_reducer.notify_one();
}

void do_accumulate_job(int R) {
   std::multimap<int, std::string>::iterator itr;
   for (int nPartition = 1; nPartition <= R; nPartition++) {
       for (itr = MergedlinesInFile.begin(); (itr != MergedlinesInFile.end()); ++itr)
       {
           if (itr->first != nPartition) continue;
              const std::string lower_sline = boost::algorithm::to_lower_copy(itr->second);
              std::vector<std::string> words;
              // split into words array with delimiter '-'. So ACT - 1 would be stored as word[0]=ACT and word[1]=1 
              boost::split(words, lower_sline, boost::is_any_of("-"));
              // check if words exist add 1 to accumualte else add word to list.
              if (wordCountMap.find(words[0]) == wordCountMap.end())
              // add word and count.
                  wordCountMap.insert(std::pair<std::string, std::string>(words[0], words[1]));
              else
                  wordCountMap[words[0]] = wordCountMap[words[0]] + "1";
       }
   }
}

void resetAccumulator() {
     wordCountMap.clear();
}

int runReducer() {
    HINSTANCE hDLLReducer;
    const wchar_t* libNameReducer = L"CReducerLibrary";
    hDLLReducer = LoadLibraryEx(libNameReducer, NULL, NULL); // Handle to DLL
    if (hDLLReducer != NULL)
    {
        CREATE_REDUCER_OBJ  ReducerPtr = (CREATE_REDUCER_OBJ)GetProcAddress(hDLLReducer, "CreateReducerObject");
        ICReducer* pReducer = ReducerPtr();

        if (pReducer != NULL)
        {
            pReducer->Reduce(wordCountMap);
            std::string fname = pReducer->getReducerFileName();
            FileManager.writeOutputFile(pReducer->getReducerFileName(), pReducer->exportData());
            // clear buffer 
            pReducer->bufferFlush();
            FreeLibrary(hDLLReducer);
        }
        else
            std::cout << "Did not load reducer Library correctly." << std::endl;
        return 1;
    }
    else {
        std::cout << "Reducer Library load failed!" << std::endl;
        return 2;
    }
}

int main(int argc, char* argv[])
{
    int R = 0;
   /*
    1)	Your program will accept three inputs via command-line:
    a)	Directory that holds input files.
    b)	Directory to hold output files.
    c)	Temporary directory to hold intermediate output files.
   */
    if (argc != 4)
    {
        std::cout << "Usage: wordcount <DIRECTORY FOR INPUT FILE> <DIRECTORY FOR OUTPUT FILE> <DIRECTORY FOR TEMP FILE> " << std::endl;
        return 1;
    }
  
    /* verify input directory.*/
    if (FileManager.isValidDirectory(argv[1]))
        FileManager.setInputFileDirectory(argv[1]);
    else
    {
        std::cout << "Please enter a valid Input directory" << std::endl;
        return 1;
    }

    /* verify output directory.*/
    if (FileManager.isValidDirectory(argv[2]))
        FileManager.setOuputFileDirectory(argv[2]);
    else
    {
        std::cout << "Please enter a valid Output directory" << std::endl;
        return 1;
    }

    /* verify temp directory.*/
    if (FileManager.isValidDirectory(argv[3]))
        FileManager.setTempFileDirectory(argv[3]);
    else
    {
        std::cout << "Please enter a valid Temp directory" << std::endl;
        return 1;
    }

    // clean temp directory.
    std::string cleanTempDirCmd = "del " + FileManager.getTempFileDirectory() + "\\PART0000*.txt";
    std::cout << cleanTempDirCmd << std::endl;
    system(cleanTempDirCmd.c_str());

    // clean output directory.
    std::string cleanOutputDirCmd = "del " + FileManager.getOuputFileDirectory() + "\\reduce*.txt";
    std::cout << cleanOutputDirCmd << std::endl;
    system(cleanOutputDirCmd.c_str());

    cleanOutputDirCmd = "del " + FileManager.getOuputFileDirectory() + "\\SUCCESS.txt";
    std::cout << cleanOutputDirCmd << std::endl;
    system(cleanOutputDirCmd.c_str());

   /*******************************  MAP  ***********************************/
   /*
   a)	Map: Is given data from a file (does not parse the file itself)
   */
    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << "Mapper is now running....." << std::endl;
    std::cout << "==========================" << std::endl;
    std::cout << std::endl;

    std::string tempfilename = "PART0000_";

    // Get partition value. 
    int Input;
    std::cout << " Enter a value for Partition (R Bucket) ";
    std::cin >> Input;
    R = Input;

    // Partition files to read. 
    FileManager.readDirectory(FileManager.getInputFileDirectory());
    // read files from input directory.
    std::vector<std::string> listOfFiles = FileManager.getFilesInDirectory();

     // Caluculate the chunk size each partition should process.  
    int totFiles = listOfFiles.size();
    int chunks = abs(totFiles / R);

    // Partition files to process based on the partition key using R. 
    for (int i = 0, ctr = 0; (ctr < totFiles && i < R); i++) {
       for (int j = 0; j < chunks; j++){
           if (ctr >= totFiles) break;
              filesInBatch.insert(std::pair<int, std::string>(i+1, listOfFiles.at(ctr++)));
           }
     }
     
    int batchsize = filesInBatch.size();
    if (batchsize < totFiles)
    {
       int i = 1; // index starts at 1.
       while (batchsize < totFiles) {
            filesInBatch.insert(std::pair<int, std::string>(i++, listOfFiles.at(batchsize++)));
          }
    }
    // End of partitioning.
    
    // call Mapper using threads for R partition.
    std::vector<std::thread> map_threads;
    for (int i = 1; i <= R; i++) {
       // add partition i to filename. 
          std::string outputfile = tempfilename + std::to_string(i) + ".txt";
          map_threads.emplace_back(std::thread(runMapper,i));
    }
   
    for(auto& t : map_threads)
      t.join();

   /*******************************  REDUCER  ***********************************/
   /* Reducer uses Reduce function from CReducerLibrary DLL.                    */
   /*****************************************************************************/
    FileManager.readDirectory(FileManager.getTempFileDirectory());
    listOfFilesTemp = FileManager.getFilesInDirectory();

    std::vector<std::thread> reducer_threads;
    for (int i = 1; i <= R; i++) {
        std::cout << "Thread :" << std::to_string(i) << "Processing File :" << listOfFilesTemp[i - 1] << std::endl;
            // map_threads.emplace_back(std::thread(runMapper, i));
            reducer_threads.emplace_back(std::thread(loadPartitions,i)); // partition starts are 1. 
    }
    for (auto& tr : reducer_threads)
        tr.join();
    
    // Do accumulation. 
    resetAccumulator();
    std::cout << "Sorting and Accumulation in process" << std::endl;
    do_accumulate_job(R);
    std::cout << "Sorting and Accumulation complete" << std::endl;
    // run reducer to produce reducer output file.
    std::cout << "Reducer DLL process is now running " << std::endl;
    runReducer();
    // Write Success File.
    FileManager.writeEmptySuccessfile();
    std::cout << "Map Reduce Process finished. " << std::endl;
    return 0;
}
