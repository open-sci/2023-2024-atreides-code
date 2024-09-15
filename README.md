# 2023-2024-atreides-code
The repository for the team Atreides of the Open Science course a.a. 2023/2024

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.11262417.svg)](https://doi.org/10.5281/zenodo.13764657)
[![SWH](https://archive.softwareheritage.org/badge/origin/https://github.com/open-sci/2023-2024-atreides-code/)](https://archive.softwareheritage.org/browse/origin/?origin_url=https://github.com/open-sci/2023-2024-atreides-code)

## Usage

### Installation

**Note**: Python 3.11.5 or previous versions are required to run the software.

```sh
# Clone the repository
git clone https://github.com/open-sci/2023-2024-atreides-code

# Move to the repository folder
cd 2023-2024-atreides-code

# Install the package and dependencies
pip install .
```

### Run the software

Create the necessary datasets ('IRIS in Meta' and 'IRIS in Index' are essential to answer the research questions) by running the following command:

```sh
python3 scripts/create_datasets.py -meta <path_to_meta_zip> -iris <path_to_iris_zip> [-index <path_to_index_zip>] <dataset_of_choice>
```

#### Arguments

- ```-meta, --meta_path```:	Required. The path to the zip file containing the OpenCitations Meta dump.
- ```-iris, --iris_path```:	Required. The path to the folder containing the IRIS CSV files.
- ```-index, --index_path```:	The path to the OpenCitations Index dump folder.
- ```--iris_in_index```:	Create the "Iris In Index" dataset, which contains all the entities with external IDs in IRIS that are in the OpenCitations Index.
- ```--iris_in_meta```:	Create the "Iris In Meta" dataset, which contains all the entities with external IDs in IRIS that are in Meta.
- ```--iris_not_in_meta```:	Create the "Iris Not In Meta" dataset, which contains all the entities with external IDs in IRIS that are not in Meta.
- ```--iris_no_id```:	Create the "Iris No ID" dataset, which contains all the entities with no external IDs in IRIS.
- ```--search_for_titles```:	Search for the entities without an ID in IRIS by their title in Meta. This can take around 3 hours to complete.

Alternatively, you can download the processed datasets from the links provided below and place them in the 'data/' directory of the repository folder.


Use the following command to get the answers to the research questions:

```sh
python3 scripts/answer_research_questions.py [-rq <research_question_number>]
```

You can get the answer to a specific research question by adding to the previous command the ```-rq``` flag followed by the number of the research question you want the answer of. By omitting this flag, you will get the answers to all the research questions.

For more detailed guidelines consult the protocol for the software:

[![protocols.io](https://a11ybadges.com/badge?logo=protocolsdotio)](https://dx.doi.org/10.17504/protocols.io.3byl497wjgo5/v5)


## Research questions:

1) What is the coverage of the publications available in IRIS, that strictly concern research conducted within the University of Bologna, in OpenCitations Meta?
2) What are the types of publications that are better covered in OpenCitations Meta?
3) What is the amount of citations (according to OpenCitations Index) the IRIS publications included in OpenCitations Meta are involved in (as citing entity and as cited entity)?
4) How many of these citations come from and go to publications not included in IRIS?
5) How many of these citations involve publications in IRIS as both citing and cited entities?

## Download original datasets

- IRIS dump (14 March 2024): [https://doi.org/10.6092/unibo%2Famsacta%2F7608](https://doi.org/10.6092/unibo%2Famsacta%2F7608)

- OpenCitations Meta April 2024 Dump: [https://doi.org/10.6084/m9.figshare.21747461.v8](https://doi.org/10.6084/m9.figshare.21747461.v8)

- OpenCitations Index November 2023 Dump: [https://doi.org/10.6084/m9.figshare.24356626.v2](https://doi.org/10.6084/m9.figshare.24356626.v2)



## Result datasets

- IRIS in Meta: [https://doi.org/10.6084/m9.figshare.25879420.v2](https://doi.org/10.6084/m9.figshare.25879420.v2)

- IRIS in Index: [https://doi.org/10.6084/m9.figshare.25879441.v2](https://doi.org/10.6084/m9.figshare.25879441.v2)

- IRIS Not in Meta: [https://doi.org/10.6084/m9.figshare.25897708.v2](https://doi.org/10.6084/m9.figshare.25897708.v2)

- IRIS No ID: [https://doi.org/10.6084/m9.figshare.25897759.v2](https://doi.org/10.6084/m9.figshare.25897759.v2)
