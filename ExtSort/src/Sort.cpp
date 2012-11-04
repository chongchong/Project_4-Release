
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#include "heapfile.h"
#include "scan.h"

#include "Sort.h"


static int _sortKeyOffset = 0;
static TupleOrder _sortOrder;

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
	_inFile = inFile; // STRCPY?
	_outFile = outFile;
	_fieldSizes = fieldSizes;
	_sortKeyIndex = sortKeyIndex;
	_sortType = fieldTypes[_sortKeyIndex];
	_sortOrder = sortOrder;
	//ascending = sortOrder == Ascending; // delete
	//sortInt = fieldTypes[_sortKeyIndex] == attrInteger;

	// Pass 0
	int numTempFiles = 0;
	if (PassZero(numTempFiles) != OK) { std::cerr << "PassZero failed." << std::endl; return; }
	if (numTempFiles == 1) { // done, write out
		RecordID rid; char *recPtr = (char *)malloc(_recLength); int recLen = _recLength;
		char *fileName = CreateTempFilename(_outFile,0,1);
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
		free(recPtr);
		passZeroFile.DeleteFile();
		s = OK;
	} else { // more passes
		if (numTempFiles <= _numBufPages -1 /* cw474: equal ??? */ ) { // only need one merge

		}

	}

	// Write out
	// deallocate the filename string pointed by the returned pointer after using it
	// Also destroy the temporary HeapFile object when you have finished writing it out.
}

Status Sort::PassZero(int &numTempFiles) {
	// Get input file
	Status status;
	HeapFile inputFile(_inFile, status);
	if (status != OK) return ReturnFAIL("Opening input file in PassZero function failed.");
	int numRecords = inputFile.GetNumOfRecords(); 
	int recCounter = 0;
	//std::cout << "num of records is " << numRecords << std::endl;

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
			recCounter++;
			// add to memory
			if (memcpy(areaPtr,recPtr,recLen) != areaPtr) 
				return ReturnFAIL("Reading records to memory in PassZero function failed.");
			areaPtr += recLen;
			areaSize -= recLen;
			if (areaSize < _recLength || recCounter == numRecords) { // can't fit another rec or all recs have been added
				// sort
				passZeroRuns++;
				switch (_sortType) {
					case attrInteger:
						std::qsort(area,numRecForSort,_recLength,CompareInt);
						break;
					case attrString:
						std::qsort(area,numRecForSort,_recLength,CompareString);
					default:
						break;
				}
				// write out
				char *fileName = CreateTempFilename(_outFile,0,passZeroRuns);
				HeapFile *tempFile =  new HeapFile(fileName,status); // NO FREEING, need it later
				if (status != OK) return ReturnFAIL("Opening temp file in PassZero function failed.");
				areaPtr = area;
				while (recCounter > 0) { // insert
					tempFile->InsertRecord(areaPtr,_recLength,rid);
					recCounter--;
					areaPtr += _recLength;
				}
				numTempFiles++;
				areaPtr = area; // reset
				areaSize = MINIBASE_PAGESIZE * _numBufPages;
				delete fileName;
			}
		}
	}

	free(area);
	free(recPtr);
	return OK;
}

Status Sort::PassOneAndBeyond(int numFiles) {
	return FAIL;
}

Status Sort::MergeManyToOne(unsigned int numSourceFiles, HeapFile **source, HeapFile *dest) {
	return FAIL;
}

Status Sort::OneMergePass(int numStartFiles, int numPass, int &numEndFiles) {
	return FAIL;
}

int Sort::CompareInt(const void *a, const void *b) { // same as compare string
		const char *sa = (const char *)a+_sortKeyOffset; 
		const char *sb = (const char *)b+_sortKeyOffset;
		//const int *ia = (int *)((*aa)+_sortKeyOffset);
		//const int *ib = (int *)((*bb)+_sortKeyOffset);
		//return _sortOrder == Ascending ? *ia - *ib : *ib - *ia; 
		return _sortOrder == Ascending ? strcmp(sa, sb) : strcmp(sb, sa);
	}

int Sort::CompareString(const void *a, const void *b) {
		const char *sa = (const char *)a+_sortKeyOffset;
		const char *sb = (const char *)b+_sortKeyOffset;
		//const char *sa = ((*aa)+_sortKeyOffset);
		//const char *sb = ((*bb)+_sortKeyOffset);
		return _sortOrder == Ascending ? strcmp(sa, sb) : strcmp(sb, sa);
}

Status Sort::ReturnFAIL(char *message) {
	std::cerr << message << "\n" << std::endl;
	return FAIL;
}
