#!/bin/bash
# compiled tpcds-kit for tpcds unit test
git clone https://github.com/CodingCat/tpcds-kit.git $1
cd $1/tools;mv Makefile.suite Makefile;make;