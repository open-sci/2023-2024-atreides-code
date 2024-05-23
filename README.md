# 2023-2024-atreides-code
The repository for the team Atreides of the Open Science course a.a. 2023/2024

## Research questions:

1) What is the coverage of the publications available in IRIS, that strictly concern research conducted within the University of Bologna, in OpenCitations Meta?
2) What is the types of publications that are better covered in OpenCitations Meta?
3) What is the amount of citations (according to OpenCitations Index) the IRIS publications included in OpenCitations Meta are involved in (as citing entity and as cited entity)?
4) How many of these citations come from and go to publications not included in IRIS, and how many of these citations involves publications in IRIS as both citing and cited entities?

## Download data

-IRIS dump (14 March 2024): [https://doi.org/10.6092/unibo%2Famsacta%2F7608](https://doi.org/10.6092/unibo%2Famsacta%2F7608)

-OpenCitations Meta April 2024 Dump: [https://doi.org/10.6084/m9.figshare.21747461.v8](https://doi.org/10.6084/m9.figshare.21747461.v8)

-OpenCitations Index November 2023 Dump: [https://doi.org/10.6084/m9.figshare.24356626.v2](https://doi.org/10.6084/m9.figshare.24356626.v2)

Save these files in a new folder "data", to store in the same path of this software.

## Reproduce the experiment

Check the dependencies in *requirements.txt*

Reach the folder containing the *answer_research_questions.py* file. 

Use the following command to run the script:

```python answer_research_questions.py```

If you want to specify a research question you can run ```python answer_research_questions.py``` adding ```-rq1``` for retrieving the results of research question 1, and so on.

## Result dataset

-IRIS in Meta: [dx.doi.org/10.6084/m9.figshare.25879420](dx.doi.org/10.6084/m9.figshare.25879420)

-IRIS in Index: [dx.doi.org/10.6084/m9.figshare.25879441](dx.doi.org/10.6084/m9.figshare.25879441)

## Identifiers
-swh:1:dir:1d8ed32ba0724089bcb7e9b4d609f60e50c18121

-doi:10.5281/zenodo.11262417

