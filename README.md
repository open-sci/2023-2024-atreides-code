# 2023-2024-atreides-code
The repository for the team Atreides of the Open Science course a.a. 2023/2024

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.11262416.svg)](https://doi.org/10.5281/zenodo.11262416)
[![SWH](https://archive.softwareheritage.org/badge/origin/https://github.com/open-sci/2023-2024-atreides-code/)](https://archive.softwareheritage.org/browse/origin/?origin_url=https://github.com/open-sci/2023-2024-atreides-code)

## Usage
### Installation

```sh
# Clone the repository
git clone https://github.com/open-sci/2023-2024-atreides-code

# Move to the repository folder
cd 2023-2024-atreides-code

# Install required dependencies using uv
# uv install options: https://docs.astral.sh/uv/getting-started/installation/
uv sync

# Activate the virtual environment
source .venv/bin/activate
```

### Run the software

Create the necessary datasets ('IRIS in Meta' and 'IRIS in Index' are required to answer the research questions) by running the following command:

```sh
python3 -m scripts.create_datasets -meta <path_to_meta_zip> -iris <path_to_iris_zip> [-index <path_to_index_zip>] <dataset_of_choice> [--year_cutoff <year>]
```

#### Arguments

- `-meta, --meta_path`: Required. The path to the folder (or zip file) containing the OpenCitations Meta dump.
- `-iris, --iris_path`: Required. The path to the folder (or zip file) containing the IRIS CSV files.
- `-index, --index_path`: The path to the OpenCitations Index dump folder (or zip).
- `-iim, --iris_in_meta`: Create the "Iris In Meta" dataset, which contains all the entities with external IDs in IRIS that are in Meta.
- `-iii, --iris_in_index`: Create the "Iris In Index" dataset, which contains all the entities with external IDs in IRIS that are in the OpenCitations Index.
- `-inim, --iris_not_in_meta`: Create the "Iris Not In Meta" dataset, which contains all the entities with external IDs in IRIS that are not in Meta.
- `-inoid, --iris_no_id`: Create the "Iris No ID" dataset, which contains all the entities with no external IDs in IRIS.
- `-yc, --year-cutoff`: (Optional) Specify a year cutoff for the mapping of IRIS data. Only entities published prior or during this year will be included in the new datasets.
- `--search_for_titles`: (Experimental) Try to reconcile the IRIS entities without PIDs using their title in OC Meta. <ins>This can take around 3 hours to complete.</ins>

Alternatively, you can download the processed datasets from the links provided below and place them in the `data/` directory of the repository folder.

Use the following command to get the answers to the research questions:

```sh
python3 -m scripts.answer_research_questions [-rq <research_question_number>]
```

- `-rq <research_question_number>`: (Optional) Specify the research question number to answer a specific question.


For more detailed guidelines consult the protocol for the software:

[![protocols.io](https://a11ybadges.com/badge?logo=protocolsdotio)](https://dx.doi.org/10.17504/protocols.io.3byl497wjgo5)


## Research questions:

1) What is the coverage of the publications available in IRIS, that strictly concern research conducted within the University of Bologna, in OpenCitations Meta?
2) What are the types of publications that are better covered in OpenCitations Meta?
3) What is the amount of citations (according to OpenCitations Index) the IRIS publications included in OpenCitations Meta are involved in (as citing entity and as cited entity)?
4) How many of these citations come from and go to publications not included in IRIS?
5) How many of these citations involve publications in IRIS as both citing and cited entities?

## Download original datasets

- UNIBO IRIS bibliographic data dump, dated 30 May 2025, updated on 3 July 2025: [https://doi.org/10.6092/unibo/amsacta/8427](https://doi.org/10.6092/unibo/amsacta/8427)

- OpenCitations Meta CSV dataset of all bibliographic metadata (June 2025): [https://doi.org/10.5281/zenodo.15625651](https://doi.org/10.5281/zenodo.15625651)

- OpenCitations Index CSV dataset of all the citation data (July 2025): [https://doi.org/10.6084/m9.figshare.24356626.v6](https://doi.org/10.6084/m9.figshare.24356626.v6)

## Output datasets

- IRIS in Meta: [https://doi.org/10.6084/m9.figshare.25879420.v2](https://doi.org/10.6084/m9.figshare.25879420.v2)

- IRIS in Index: [https://doi.org/10.6084/m9.figshare.25879441.v2](https://doi.org/10.6084/m9.figshare.25879441.v2)

- IRIS Not in Meta: [https://doi.org/10.6084/m9.figshare.25897708.v2](https://doi.org/10.6084/m9.figshare.25897708.v2)

- IRIS No ID: [https://doi.org/10.6084/m9.figshare.25897759.v2](https://doi.org/10.6084/m9.figshare.25897759.v2)
