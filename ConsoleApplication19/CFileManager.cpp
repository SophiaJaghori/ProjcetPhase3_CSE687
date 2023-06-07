#include "CFileManager.h"
using namespace boost::filesystem;

void CFileManager::setTempFileDirectory(std::string tempDir) {_tempDir = tempDir;};
void CFileManager::setOuputFileDirectory(std::string outputDir) {_outputDir=outputDir;};
void CFileManager::setInputFileDirectory(std::string inputDir) {_inputDir=inputDir;};

std::string CFileManager::getTempFileDirectory() {return _tempDir;};
std::string CFileManager::getOuputFileDirectory() {return _outputDir;}
std::string CFileManager::getInputFileDirectory() {return _inputDir;};

bool CFileManager::isValidDirectory(std::string filenamePath)
{
    try
    {
        if (boost::filesystem::exists(filenamePath))
        {
            if (boost::filesystem::is_regular_file(filenamePath))
            {
                std::cout << filenamePath << "Not a valid directory path. Please re-enter valid directory path and try again" << std::endl;
                return false;
            }
            else if (boost::filesystem::is_directory(filenamePath))
            {
                return true;
            }
            else
            {
                std::cout << filenamePath << " Path exists, but is not a regular file or directory" << std::endl;
                return false;
            }
        }
        else
        {
            std::cout << filenamePath << " does not exist\n";
            return false;
        }
    }
    catch (filesystem_error& ex)
    {
        std::cout << ex.what() << std::endl;
    }
};

void CFileManager::readDirectory(std::string filenamePath) {
    filesInDirectory.clear();
    try
    {
            if (is_directory(filenamePath))
            { 
                std::cout <<std::endl << "Files in directory path " << filenamePath << std::endl << std::endl;

                // store file names from input directory for processing individual files.
                for (directory_entry const& fileName : directory_iterator(filenamePath))
                {
                    std::string strFileName = fileName.path().string();
                    std::cout << "file  : " << strFileName << std::endl;
                    CFileManager::filesInDirectory.push_back(strFileName);
                }
                std::cout << std::endl;
            }
    }
    catch (filesystem_error& ex)
    {
        std::cout << ex.what() << std::endl;
    }
};

std::vector<std::string> CFileManager::readLinesInFile(std::string fileName) {
    std::ifstream inFile(fileName);
    linesInFile.clear();
    std::string sline;
    if (inFile)
    {
        while (std::getline(inFile, sline)) {
            linesInFile.push_back(sline);
        }
        inFile.close();
    }
    std::cout << std::endl << "file name : " << fileName << "  -  Total Lines :" << linesInFile.size() << std::endl;
    return linesInFile;
}

std::vector<std::string> CFileManager::getFilesInDirectory() {
    return filesInDirectory;
}; 

void CFileManager::writeOutputFile(std::string filenamePath, std::string strline) {
    try
    {
        std::fstream outputfile;
        outputfile.open(filenamePath, std::ios_base::app);
        outputfile << strline << std::endl;
        outputfile.close();
    }
    catch (filesystem_error& ex)
    {
        std::cout << ex.what() << std::endl;
    }
};

void CFileManager::writeOutputFile(std::string filename, std::map<std::string, int> outputLines)
{
    // Writes output to output directory configured in filemanager.
    try
    {
        std::string filenamePath = getOuputFileDirectory() + "\\" + filename + ".txt";
        std::multimap<std::string, int>::iterator itr;
        std::string strline;
        std::string strWord;
        std::string strCount;

        for (itr = outputLines.begin(); itr != outputLines.end(); ++itr) {
            strWord = itr->first;
            strCount = std::to_string(itr->second);
            strline = strWord + " - " + strCount;
            writeOutputFile(filenamePath, strline);
        }
    }
    catch (filesystem_error& ex)
    {
        std::cout << ex.what() << std::endl;
    }
};

/*
Upon success an additional empty file SUCCESS will be written out to the output directory.
*/
void CFileManager::writeEmptySuccessfile() {
    std::string filenamePath = getOuputFileDirectory() + "\\" + "SUCCESS.txt";
    writeOutputFile(filenamePath, "SUCCESS");
};

/* To write output to temp directory. */
void CFileManager::writeTempOutputFile(std::string filename, std::multimap<std::string, int, std::less<std::string>> wordMapperLines)
{
    // writes output to temp directory configured in filemanager.
    try
    {
        std::string filenamePath = getTempFileDirectory() + "\\" + filename + ".txt";
        std::multimap<std::string, int>::iterator itr;
        std::string strline;
        std::string strWord;
        std::string strCount;

        for (itr = wordMapperLines.begin(); itr != wordMapperLines.end(); ++itr) {
            strWord = itr->first;
            strCount = std::to_string(itr->second);
            strline = strWord + " - " + strCount;
            writeOutputFile(filenamePath, strline);
        }
    }
    catch (filesystem_error& ex)
    {
        std::cout << ex.what() << std::endl;
    }
}

/* To write output to temp directory. */
void CFileManager::writeTempOutputFile(std::string filename, std::vector<std::string> wordLines)
{
    // writes output to temp directory configured in filemanager.
    try
    {
        std::string filenamePath = getTempFileDirectory() + "\\" + filename + ".txt";
        std::string strline;
        for (std::string word : wordLines) {
            {
                if (word.empty())
                    return;
                strline = word + "-1";
            }
            writeOutputFile(filenamePath, strline);
        }
    }
    catch (filesystem_error& ex)
    {
        std::cout << ex.what() << std::endl;
    }
}


