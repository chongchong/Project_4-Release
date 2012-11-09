
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#include "heapfile.h"
#include "scan.h"

#include "Sort.h"
#include <vector>
#include <tuple>
#include <algorithm>

static int _sortKeyOffset = 0;
static TupleOrder _sortOrder;
static AttrType _sortType;

//-------------------------------------------------------------------
// Sort::CreateTempFilename
//
// Input   : file_name,	The output file name of the sorting task.
//			 pass,		The number of passes (assuming the sort phase is pass 0).
//			 run,		The run number within the pass.
// Output  : None.
// Return  : The temporary file name
// Example : File 7 in pass 3 for output file FOO will be named FOO.sort.temp.3.7.
// Note    : It is your responsibility to destroy the return filename string.
//-------------------------------------------------------------------
char *Sort::CreateTempFilename(char *filename, int pass, int run)
{
	char *name = new char[strlen(filename) + 20];
	sprintf(name,"%s.sort.temp.%d.%d", filename, pass, run);
	return name;
}

Sort::Sort(
	char		*inFile,			// Name of unsorted heapfile.
	char		*outFile,		// Name of sorted heapfile.
	int      	numFields,		// Number of fields in input records.
	AttrType 	fieldTypes[],	// Array containing field types of input records.
	// i.e. index of in[] ranges from 0 to (len_in - 1)
	short    	fieldSizes[],	// Array containing field sizes of input records.
	int       	sortKeyIndex,	// The number of the field to sort on.
	// fld_no ranges from 0 to (len_in - 1).
	TupleOrder 	sortOrder,		// ASCENDING, DESCENDING
	int       	numBufPages,	// Number of buffer pages available for sorting.
	Status 	&s)
{
	// Initialize private instance variables
	_recLength = 0;
	for (int i=0;i<numFields;i++) {
		_recLength += fieldSizes[i];
	}
	for (int i=0;i<sortKeyIndex;i++) {
		_sortKeyOffset += fieldSizes[i];
	}
	_numBufPages = numBufPages;
	_inFile = inFile; 
	_outFile = outFile;
	_fieldSizes = fieldSizes;
	_sortKeyIndex = sortKeyIndex;
	_sortType = fieldTypes[_sortKeyIndex];
	_sortOrder = sortOrder;

	// Pass 0
	int numTempFiles = 0;
	if (PassZero(numTempFiles) != OK) { std::cerr << "PassZero failed." << std::endl; return; }
	if (numTempFiles == 1) { // done, write out
		RecordID rid; char *recPtr = (char *)malloc(_recLength); int recLen = _recLength;
		char *fileName = CreateTempFilename(_outFile,0,0);
		HeapFile passZeroFile(fileName,s); // read temp file
		if (s != OK) { std::cerr << "Opening PassZero temp file failed." << std::endl; return; }
		Scan *scan = passZeroFile.OpenScan(s);
		if (s != OK) { std::cerr << "Opening scan in PassZero failed." << std::endl; return; }
		HeapFile output(_outFile, s);
		if (s != OK) { std::cerr << "Opening output file in PassZero failed." << std::endl; return; }
		while (scan->GetNext(rid,recPtr,recLen) == OK) {
			output.InsertRecord(recPtr,recLen,rid);
		}
		delete fileName;
		delete scan;
		free(recPtr);
		passZeroFile.DeleteFile();
		s = OK;
	} else { // more passes
		if (PassOneAndBeyond(numTempFiles) != OK) { std::cerr << "PassOneAndBeyond failed." << std::endl; return; }
		s = OK;
	}
}

//-------------------------------------------------------------------
// Sort::PassZero
//
// Input   : None
// Output  : Number of temp files generated after this pass
// Return  : OK if Pass 0 succeeds, FAIL otherwise
//-------------------------------------------------------------------
Status Sort::PassZero(int &numTempFiles) {
	// Get input file
	Status status;
	HeapFile inputFile(_inFile, status);
	if (status != OK) return ReturnFAIL("Opening input file in PassZero function failed.");
	int numRecords = inputFile.GetNumOfRecords(); 
	int recCounter = 0, globalRecCounter = 0;

	// Allocate memory
	int areaSize = MINIBASE_PAGESIZE * _numBufPages;
	char *area = (char *)malloc(areaSize);
	char *areaPtr = area;
	RecordID rid; char *recPtr = (char *)malloc(_recLength); int recLen = _recLength;
	int numRecForSort = std::min(areaSize/_recLength,numRecords); // number of rec in sorting area at once

	// Open Scan
	Scan *scan = inputFile.OpenScan(status); 
	if (status != OK) return ReturnFAIL("Opening scan in PassZero function failed.");
	
	// Sort
	passZeroRuns = 0;
	if (areaSize >= _recLength) { // can fit at least one record
		while (scan->GetNext(rid,recPtr,recLen) == OK) {
			recCounter++; globalRecCounter++;
			// add to memory
			if (memcpy(areaPtr,recPtr,recLen) != areaPtr) 
				return ReturnFAIL("Reading records to memory in PassZero function failed.");
			areaPtr += recLen;
			areaSize -= recLen;
			if (areaSize < _recLength || globalRecCounter == numRecords) { // can't fit another rec or all recs have been added
				// sort
				switch (_sortType) {
					case attrInteger:
						std::qsort(area,recCounter,_recLength,CompareInt); 
						break;
					case attrString:
						std::qsort(area,recCounter,_recLength,CompareString); 
					default:
						break;
				}
				// write out
				char *fileName = CreateTempFilename(_outFile,0,passZeroRuns);
				passZeroRuns++;
				HeapFile *tempFile =  new HeapFile(fileName,status); 
				if (status != OK) return ReturnFAIL("Opening temp file in PassZero function failed.");
				areaPtr = area;
				while (recCounter > 0) { // insert
					tempFile->InsertRecord(areaPtr,_recLength,rid);
					recCounter--;
					areaPtr += _recLength;
				}
				numTempFiles++;
				areaSize = MINIBASE_PAGESIZE * _numBufPages;
				areaPtr = area; // reset
				delete fileName;
				delete tempFile; 
			}
		}
	}

	free(area);
	free(recPtr);
	delete scan;
	return OK;
}

//-------------------------------------------------------------------
// Sort::PassOneAndBeyond
//
// Input   : Number of files generated by Pass 0
// Output  : None
// Return  : OK if Pass 1 and beyond succeed, FAIL otherwise
//-------------------------------------------------------------------
Status Sort::PassOneAndBeyond(int numFiles) {
	passOneBeyondRuns = 0;
	int numPass = 1, numStartFiles = passZeroRuns, numEndFiles;
	do { 
		numEndFiles = 0;
		if (OneMergePass(numStartFiles, numPass, numEndFiles) != OK ) return ReturnFAIL("OneMergePass failed.");
		numStartFiles = numEndFiles;
		numPass++;
	} while (numEndFiles > 1);

	// Write out
	Status s;
	RecordID rid; char *recPtr = (char *)malloc(_recLength); int recLen = _recLength;
	char *fileName = CreateTempFilename(_outFile,numPass-1,passOneBeyondRuns);
	HeapFile file(fileName,s); // read temp file
	if (s != OK) return ReturnFAIL("Opening PassOneAndBeyond temp file failed.");
	Scan *scan = file.OpenScan(s);
	if (s != OK) return ReturnFAIL("Opening scan in PassOneAndBeyond failed.");
	HeapFile output(_outFile, s);
	if (s != OK) return ReturnFAIL("Opening output file in PassOneAndBeyond failed.");
	while (scan->GetNext(rid,recPtr,recLen) == OK) {
		output.InsertRecord(recPtr,recLen,rid);
	}
	delete fileName;
	delete scan;
	free(recPtr);
	file.DeleteFile();
	return OK;
}

//-------------------------------------------------------------------
// Sort::MergeManyToOne
//
// Input   : numSourceFiles,	number of files needed to be merged
//			 source,			array of heap files to be merged
//			 dest,				output heap file
// Output  : None
// Return  : OK if merging succeeds, FAIL otherwise
//-------------------------------------------------------------------
Status Sort::MergeManyToOne(unsigned int numSourceFiles, HeapFile **source, HeapFile *dest) {
	// Open a scan for each source file
	Status status;
	int numOfRecords = 0, recCounter = 0;
	Scan **scanners = new Scan *[numSourceFiles];
	RecordID rid; char *recPtr = (char *)malloc(_recLength); int recLen = _recLength;
	for (int i=0; i<numSourceFiles; i++) {
		HeapFile *hf = source[i];
		Scan *scan = hf->OpenScan(status);
		if (status != OK) return ReturnFAIL("Opening scanners in MergeManyToOne failed.");
		scanners[i] = scan;
		numOfRecords += hf->GetNumOfRecords();
	}
	// Create a vector to hold (numSourceFiles) records to compare
	std::vector<std::tuple<char *, int>> values;
	char **recPtrArray = new char*[numSourceFiles];
	for (int i=0; i<numSourceFiles; i++) {
		if (scanners[i]->GetNext(rid,recPtr,recLen) != OK)
			return ReturnFAIL("Error scanning files in MergeManyToOne");
		recPtrArray[i] = (char *)malloc(recLen);
		memcpy(recPtrArray[i],recPtr,recLen);
		values.push_back(std::make_tuple(recPtrArray[i],i));
	}
	while (recCounter < numOfRecords) { // merge till all records are written out
		std::sort(values.begin(),values.end(),&CompareForMerge); // sort vector
		std::tuple<char *,int> first = values.front(); // get smallest/largest in vector
		int firstIndex = std::get<1>(first);
		dest->InsertRecord(std::get<0>(first),recLen,rid); // add it to output file
		recCounter++;
		status = scanners[firstIndex]->GetNext(rid,recPtr,recLen); // try to move pointer
		if (status == FAIL) return ReturnFAIL("Error scanning files in MergeManyToOne");
		else if (status == OK) { // if there are still records in that file
			memcpy(recPtrArray[firstIndex],recPtr,recLen);
			values.push_back(std::make_tuple(recPtrArray[firstIndex],firstIndex)); // add new record to vector
		}
		values.erase(values.begin()); // erase first
	}
	free(recPtr);
	for (int i=0;i<numSourceFiles; i++) {
		delete scanners[i];
	}
	for (int i=0;i<numSourceFiles;i++) {
		free(recPtrArray[i]);
	}
	delete scanners;
	delete recPtrArray;
	return OK;
}

//-------------------------------------------------------------------
// Sort::OneMergePass
//
// Input   : numStartFiles,	number of files generated in the last pass
//			 numPass,		the number of the current pass
// Output  : Number of temp files generated after this pass
// Return  : OK if this pass succeeds, FAIL otherwise
//-------------------------------------------------------------------
Status Sort::OneMergePass(int numStartFiles, int numPass, int &numEndFiles) {
	// fileCounter:  number of files generated in the previous pass
	// numOfRuns:  number of runs of this pass
	int fileCounter = 0, runCounter = 0, numOfRuns = std::ceilf((float)numStartFiles/(_numBufPages - 1));
	Status status;
	while (runCounter < numOfRuns) {
		// For each run, read in at most (numBufPages - 1) pages at a time
		int numPagesToRead = std::min(numStartFiles - fileCounter, _numBufPages - 1);
		// Create an array of heap files
		HeapFile **filesToMerge = new HeapFile *[numPagesToRead];
		for (int i=0; i<numPagesToRead; i++) { // add files into array
			char *filename = CreateTempFilename(_outFile, numPass-1, fileCounter);
			HeapFile *hf = new HeapFile(filename, status);
			if (status != OK) return ReturnFAIL("Unable to read a file in OneMergePass.");
			filesToMerge[i] = hf;
			fileCounter++;
		}
		// Create an output temp file for this run
		char *filename = CreateTempFilename(_outFile, numPass, runCounter);
		HeapFile *dest = new HeapFile(filename, status);
		passOneBeyondRuns = runCounter;
		runCounter++; 
		if (status != OK) return ReturnFAIL("Failed to create a file in OneMergePass.");
		// Merge
		if (MergeManyToOne(numPagesToRead,filesToMerge,dest) != OK) return ReturnFAIL("Failed to merge in OneMergePass.");
		numEndFiles++;
		for (int i=0;i<numPagesToRead;i++){
			delete filesToMerge[i];
		}
		delete dest;
		delete filesToMerge;
		delete filename;
	}
	return OK;
}

//-------------------------------------------------------------------
// Sort::CompareInt
//
// Input   : Pointers to two records
// Output  : None
// Return  : -1, 0, or 1 depending on the comparison result
//-------------------------------------------------------------------
int Sort::CompareInt(const void *a, const void *b) { 
	const char *sa = (const char *)a+_sortKeyOffset; 
	const char *sb = (const char *)b+_sortKeyOffset;
	int ia= atoi(sa);
	int ib= atoi(sb);
	return _sortOrder == Ascending ? (ia-ib) : (ib-ia);
}

//-------------------------------------------------------------------
// Sort::CompareString
//
// Input   : Pointers to two records
// Output  : None
// Return  : -1, 0, or 1 depending on the comparison result
//-------------------------------------------------------------------
int Sort::CompareString(const void *a, const void *b) {
	const char *sa = (const char *)a+_sortKeyOffset;
	const char *sb = (const char *)b+_sortKeyOffset;
	return _sortOrder == Ascending ? strcmp(sa, sb) : strcmp(sb, sa);
}

//-------------------------------------------------------------------
// Sort::CompareForMerge
//
// Input   : Two tuples containint <record pointer, array index>
// Output  : None
// Return  : The comparison result
//-------------------------------------------------------------------
bool Sort::CompareForMerge(std::tuple<char *,int>& t1, std::tuple<char *,int>& t2) { 
	switch (_sortType) {
		case attrInteger:
			return CompareInt(std::get<0>(t1),std::get<0>(t2))<0;
			break;
		case attrString:
			return CompareString(std::get<0>(t1),std::get<0>(t2))<0;
		default:
			break;
	}
}

//-------------------------------------------------------------------
// Sort::ReturnFAIL
//
// Input   : Failure message
// Output  : None
// Return  : FAIL
//-------------------------------------------------------------------
Status Sort::ReturnFAIL(char *message) {
	std::cerr << message << "\n" << std::endl;
	return FAIL;
}
