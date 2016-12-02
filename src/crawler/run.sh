#!/bin/bash

#---------------------------------------------
#crawler parameters. should be set before using

#name of program
BIN=crawler

HOST="http://www.reuters.com/"

#for fetching articles
NUM_THREAD_ARTICLE=16

#BFS to find archive names
NUM_THREAD_ARCHIVE=1

#for fetching archive list pages
NUM_THREAD_LIST=4

#approximate maximum
NUM_MAX_DOC=100

WORK_DIR="./"

#separate with space. empty for all archive
TARGET_ARCHIVE_LIST=\
"/news/archive/worldNews \
/news/archive/businessNews \
/news/archive/technologyNews"

LOG_FILE=${WORK_DIR}Log

#---------------------------------------------
DIR_DOCS="./docs"

if [ ! -d $DIR_DOCS ]; then
  mkdir $DIR_DOCS
fi

#---------------------------------------------
#clear history data (optional)
rm -f ${DIR_DOCS}/*

#running
$(${WORK_DIR}${BIN} \
	$HOST \
	$NUM_THREAD_ARTICLE \
	$NUM_THREAD_ARCHIVE \
	$NUM_THREAD_LIST \
	$NUM_MAX_DOC \
	$WORK_DIR \
	$TARGET_ARCHIVE_LIST \
	> $LOG_FILE)