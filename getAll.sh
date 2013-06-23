#!/bin/bash
python crawlMarket.py all_`date +%Y%m%d%H%M%S`.db `python getCategories.py`
