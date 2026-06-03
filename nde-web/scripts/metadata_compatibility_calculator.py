#!/usr/bin/env python3
"""
Metadata Compatibility Calculator

This script calculates metadata compatibility averages for datasources in the NDE discovery system.
It can be run standalone using: python metadata_compatibility_calculator.py --datasource <datasource_name>

Usage:
    python metadata_compatibility_calculator.py --datasource biotools --mongo-url mongodb://su11:27017/
    python metadata_compatibility_calculator.py --datasource ncbi_sra --cache-dir ./cache/
"""

import argparse
import json
import logging
import os

from pymongo import MongoClient

# Field definitions for different datasource types
COMPUTATIONAL_TOOL_REQUIRED = [
    "date",
    "includedInDataCatalog",
    "funding",
    "author",
    "description",
    "name"
]

COMPUTATIONAL_TOOL_RECOMMENDED = [
    "citedBy",
    "doi",
    "topicCategory",
    "codeRepository",
    "programmingLanguage",
    "applicationCategory",
    "applicationSubCategory",
    "input",
    "output",
    "featureList",
    "operatingSystem",
    "softwareRequirements",
    "softwareVersion",
    "citation",
    "conditionsOfAccess",
    "dateModified",
    "interactionStatistic",
    "license",
    "identifier",
    "url"
]

COMPUTATIONAL_TOOL_REQUIRED_AUGMENTED = ['funding']
COMPUTATIONAL_TOOL_RECOMMENDED_AUGMENTED = ["citation", "topicCategory"]

RESOURCE_CATALOG_REQUIRED = [
    "date",
    "funding",
    "includedInDataCatalog",
    "measurementTechnique",
    "description",
    "name",
    "url",
    "about",
    "genre"
]

RESOURCE_CATALOG_RECOMMENDED = [
    "author",
    "citedBy",
    "doi",
    "infectiousAgent",
    "healthCondition",
    "species",
    "variableMeasured",
    "citation",
    "conditionsOfAccess",
    "dateCreated",
    "dateModified",
    "datePublished",
    "interactionStatistic",
    "isBasedOn",
    "keywords",
    "license",
    "sdPublisher",
    "spatialCoverage",
    "temporalCoverage",
    "usageInfo",
    "identifier",
    "topicCategory",
    "collectionSize",
    "hasAPI",
    "hasDownload",
    "collectionType"
]

RESOURCE_CATALOG_REQUIRED_AUGMENTED = ["funding", "measurementTechnique"]
RESOURCE_CATALOG_RECOMMENDED_AUGMENTED = [
    "species",
    "infectiousAgent",
    "healthCondition",
    "citation",
    "topicCategory"
]

DATA_COLLECTION_REQUIRED = [
    "about",
    "collectionSize",
    "date",
    "dateModified",
    "description",
    "includedInDataCatalog",
    "name",
    "url",
]

DATA_COLLECTION_RECOMMENDED = [
    "author",
    "citation",
    "conditionsOfAccess",
    "creator",
    "dateCreated",
    "datePublished",
    "exampleOfWork",
    "funding",
    "healthCondition",
    "infectiousAgent",
    "interactionStatistic",
    "isBasedOn",
    "license",
    "measurementTechnique",
    "sameAs",
    "spatialCoverage",
    "species",
    "temporalCoverage",
    "topicCategory",
    "usageInfo",
    "variableMeasured",
]

DATA_COLLECTION_REQUIRED_AUGMENTED = ["funding", "measurementTechnique"]
DATA_COLLECTION_RECOMMENDED_AUGMENTED = [
    "species",
    "infectiousAgent",
    "healthCondition",
    "citation",
    "topicCategory"
]

SAMPLE_REQUIRED_FIELDS = [
    "name",
    "url",
    "identifier",
    "includedInDataCatalog",
    "author",
    "date",
]

SAMPLE_RECOMMENDED_FIELDS = [
    "description",
    "sameAs",
    "conditionsOfAccess",
    "usageInfo",
    "license",
    "cellType",
    "infectiousAgent",
    "species",
    "healthCondition",
    "funding",
    "creditText",
    "anatomicalStructure",
    "sex",
    "developmentalStage",
    "sampleAvailability",
    "sampleProcess",
    "sampleType",
    "collectionMethod",
    "instrument",
    "collector",
    "contributor",
    "locationOfOrigin",
    "citation",
    "dateModified",
    "dateCollected",
    "dateProcessed",
    "interactionStatistic",
    "keywords",
    "sdPublisher",
    "sourceOrganization",
    "topicCategory",
]

SAMPLE_REQUIRED_AUGMENTED_FIELDS = []
SAMPLE_RECOMMENDED_AUGMENTED_FIELDS = []

DATASET_REQUIRED_FIELDS = [
    "name",
    "description",
    "author",
    "url",
    "measurementTechnique",
    "includedInDataCatalog",
    "distribution",
    "funding",
    "date",
]

DATASET_RECOMMENDED_FIELDS = [
    "dateCreated",
    "dateModified",
    "datePublished",
    "citedBy",
    "doi",
    "infectiousAgent",
    "healthCondition",
    "species",
    "variableMeasured",
    "citation",
    "conditionsOfAccess",
    "isBasedOn",
    "keywords",
    "license",
    "sdPublisher",
    "spatialCoverage",
    "temporalCoverage",
    "topicCategory",
    "identifier",
    "usageInfo",
    "interactionStatistic",
]

DATASET_REQUIRED_AUGMENTED_FIELDS = ["funding", "measurementTechnique"]
DATASET_RECOMMENDED_AUGMENTED_FIELDS = [
    "species", "infectiousAgent", "healthCondition", "citation", "topicCategory"
]

# Legacy field names for backward compatibility (these match the original REQUIRED_FIELDS/RECOMMENDED_FIELDS)
REQUIRED_FIELDS = DATASET_REQUIRED_FIELDS
RECOMMENDED_FIELDS = DATASET_RECOMMENDED_FIELDS
REQUIRED_AUGMENTED_FIELDS = DATASET_REQUIRED_AUGMENTED_FIELDS
RECOMMENDED_AUGMENTED_FIELDS = DATASET_RECOMMENDED_AUGMENTED_FIELDS


class MetadataCompatibilityCalculator:
    """Calculator for metadata compatibility metrics across datasources."""

    def __init__(self, mongo_url="mongodb://su11:27017/", cache_dir="./cache"):
        """
        Initialize the calculator.

        Args:
            mongo_url (str): MongoDB connection URL
            cache_dir (str): Directory to store cache files
        """
        self.mongo_url = mongo_url
        self.cache_dir = cache_dir
        self.setup_logging()
        self.ensure_cache_dir()

    def setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def ensure_cache_dir(self):
        """Ensure cache directory exists."""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def load_from_cache(self, datasource):
        """Load cached results for a datasource."""
        cache_file = os.path.join(self.cache_dir, f'cache_{datasource}.json')
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    averages = json.load(f)
                logging.info(f"Loaded cached results for {datasource}")
                return averages
            except (json.JSONDecodeError, IOError) as e:
                logging.warning(f"Failed to load cache for {datasource}: {e}")
        return None

    def save_to_cache(self, datasource, averages):
        """Save results to cache for a datasource."""
        cache_file = os.path.join(self.cache_dir, f'cache_{datasource}.json')
        try:
            with open(cache_file, 'w') as f:
                json.dump(averages, f, indent=2)
            logging.info(f"Saved results to cache for {datasource}")
        except IOError as e:
            logging.error(f"Failed to save cache for {datasource}: {e}")

    def calculate_conditionsOfAccess_uniformity(self, datasource, collection):
        """Calculate uniformity of conditionsOfAccess field."""
        varied_datasources = [
            "ncbi_sra",
            "hca",
            "hubmap",
            "mendeley",
            "dataverse",
        ]

        if datasource in varied_datasources:
            return "Varied"

        # Aggregate to get all unique conditionsOfAccess values
        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'uniqueValues': {'$addToSet': '$conditionsOfAccess'}
                }
            }
        ]

        try:
            result = list(collection.aggregate(pipeline))
        except Exception as e:
            logging.error(
                f"Error aggregating conditionsOfAccess for {datasource}: {e}")
            return "Unknown"

        # Check if there's exactly one unique value
        if result and len(result[0]['uniqueValues']) == 1 and result[0]['uniqueValues'][0] is not None:
            return result[0]['uniqueValues'][0]
        elif result and len(set(result[0]['uniqueValues']) - {None}) == 1:
            # Handle case where there's one unique value plus None values
            return (set(result[0]['uniqueValues']) - {None}).pop()
        elif result and len(result[0]['uniqueValues']) == 0:
            return "Unknown"
        else:
            return "Varied"

    def calculate_metadata_compatibility_average(self, datasource, use_cache=False):
        """
        Calculate metadata compatibility averages for a datasource.

        Args:
            datasource (str): Name of the datasource to analyze
            use_cache (bool): Whether to use/save cache

        Returns:
            dict: Calculated averages and metrics
        """
        print(
            f"Calculating metadata compatibility for datasource: {datasource}")
        # Check cache first if enabled
        # if use_cache:
        #     cached_averages = self.load_from_cache(datasource)
        #     if cached_averages is not None:
        #         return cached_averages
        # Select which fields to use based on datasource
        if datasource == "biotools":
            required_fields = COMPUTATIONAL_TOOL_REQUIRED
            recommended_fields = COMPUTATIONAL_TOOL_RECOMMENDED
            required_augmented_fields = COMPUTATIONAL_TOOL_REQUIRED_AUGMENTED
            recommended_augmented_fields = COMPUTATIONAL_TOOL_RECOMMENDED_AUGMENTED
        # Add resource catalog datasources here
        elif datasource in ["resource_catalog", "catalog"]:
            required_fields = RESOURCE_CATALOG_REQUIRED
            recommended_fields = RESOURCE_CATALOG_RECOMMENDED
            required_augmented_fields = RESOURCE_CATALOG_REQUIRED_AUGMENTED
            recommended_augmented_fields = RESOURCE_CATALOG_RECOMMENDED_AUGMENTED
        elif datasource in ["bv_brc", "emdb", "clingen", "mwccs", "dbaasp"]:
            print(f"Using data collection fields for {datasource}")
            required_fields = DATA_COLLECTION_REQUIRED
            recommended_fields = DATA_COLLECTION_RECOMMENDED
            required_augmented_fields = DATA_COLLECTION_REQUIRED_AUGMENTED
            recommended_augmented_fields = DATA_COLLECTION_RECOMMENDED_AUGMENTED
        elif datasource in ["ceirr", "biosample", "bei"]:
            print(f"Using sample fields for {datasource}")
            required_fields = SAMPLE_REQUIRED_FIELDS
            recommended_fields = SAMPLE_RECOMMENDED_FIELDS
            required_augmented_fields = SAMPLE_REQUIRED_AUGMENTED_FIELDS
            recommended_augmented_fields = SAMPLE_RECOMMENDED_AUGMENTED_FIELDS
        else:
            # Default to dataset fields (same as original REQUIRED_FIELDS/RECOMMENDED_FIELDS)
            required_fields = DATASET_REQUIRED_FIELDS
            recommended_fields = DATASET_RECOMMENDED_FIELDS
            required_augmented_fields = DATASET_REQUIRED_AUGMENTED_FIELDS
            recommended_augmented_fields = DATASET_RECOMMENDED_AUGMENTED_FIELDS

        # Connect to MongoDB
        try:
            client = MongoClient(self.mongo_url)
            db = client["nde_hub_src"]
            collection = db[datasource]
            logging.info(
                f"Calculating metadata compatibility average for {datasource}")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            return self._get_default_averages(required_fields, recommended_fields)

        # Build project stage dynamically
        project_stage = {
            '$project': {
                # For each required/recommended field, 1 if present, else 0
                **{
                    field: {
                        '$cond': [{'$ifNull': [f'${field}', False]}, 1, 0]
                    }
                    for field in required_fields + recommended_fields
                },
                # For "augmented" fields, check if they're in _meta arrays
                **{
                    f'{field}_presence': {
                        '$cond': [
                            {'$in': [field, '$_meta.required_augmented_fields']},
                            1,
                            0
                        ]
                    }
                    for field in required_augmented_fields
                },
                **{
                    f'{field}_presence': {
                        '$cond': [
                            {'$in': [field, '$_meta.recommended_augmented_fields']},
                            1,
                            0
                        ]
                    }
                    for field in recommended_augmented_fields
                },
                # Keep original coverage fields
                '_meta.completeness.augmented_recommended_ratio': 1,
                '_meta.completeness.augmented_required_ratio': 1,
                '_meta.completeness.recommended_score_ratio': 1,
                '_meta.completeness.required_ratio': 1,
            }
        }

        # Build the aggregation pipeline
        aggregation_pipeline = [
            project_stage,
            {
                '$group': {
                    '_id': None,
                    'avg_augmented_recommended_ratio': {
                        '$avg': '$_meta.completeness.augmented_recommended_ratio'
                    },
                    'avg_augmented_required_ratio': {
                        '$avg': '$_meta.completeness.augmented_required_ratio'
                    },
                    'avg_recommended_score_ratio': {
                        '$avg': '$_meta.completeness.recommended_score_ratio'
                    },
                    'avg_required_ratio': {
                        '$avg': '$_meta.completeness.required_ratio'
                    },
                    # Averages for each field in the required/recommended sets
                    **{
                        f'avg_{field}': {
                            '$avg': f'${field}'
                        }
                        for field in required_fields + recommended_fields
                    },
                    # Averages for the presence of augmented fields
                    **{
                        f'avg_{field}_presence': {
                            '$avg': f'${field}_presence'
                        }
                        for field in required_augmented_fields + recommended_augmented_fields
                    },
                }
            }
        ]

        try:
            result = list(collection.aggregate(aggregation_pipeline))
        except Exception as e:
            logging.error(
                f"Error running aggregation pipeline for {datasource}: {e}")
            return self._get_default_averages(required_fields, recommended_fields)

        if result:
            averages = result[0]
            del averages['_id']

            # Round numerical values
            for key in list(averages):
                if averages[key] is not None:
                    try:
                        averages[key] = round(averages[key], 4)
                    except TypeError:
                        logging.warning(
                            f"{averages[key]} for {key} cannot be rounded.")
                else:
                    logging.warning(f"Value for {key} is None, cannot round.")

            # Separate out the coverage for required & recommended fields
            required_fields_coverage = {}
            recommended_fields_coverage = {}

            for field in required_fields:
                key = f'avg_{field}'
                if key in averages:
                    required_fields_coverage[field] = averages.pop(key)

            for field in recommended_fields:
                key = f'avg_{field}'
                if key in averages:
                    recommended_fields_coverage[field] = averages.pop(key)

            sum_required_coverage = sum(required_fields_coverage.values())
            sum_recommended_coverage = sum(
                recommended_fields_coverage.values())

            averages['required_fields'] = required_fields_coverage
            averages['recommended_fields'] = recommended_fields_coverage
            averages['sum_required_coverage'] = round(sum_required_coverage, 2)
            averages['sum_recommended_coverage'] = round(
                sum_recommended_coverage, 2)

            # Extract coverage for augmented fields
            required_augmented_coverage = {}
            recommended_augmented_coverage = {}

            for field in required_augmented_fields:
                key = f'avg_{field}_presence'
                required_augmented_coverage[field] = round(
                    averages.pop(key, 0), 2)

            for field in recommended_augmented_fields:
                key = f'avg_{field}_presence'
                recommended_augmented_coverage[field] = round(
                    averages.pop(key, 0), 2)

            averages['required_augmented_fields_coverage'] = required_augmented_coverage
            averages['recommended_augmented_fields_coverage'] = recommended_augmented_coverage

            # Calculate the "binary" coverage for required & recommended fields
            binary_required_score = sum(
                1 for field in required_fields
                if required_fields_coverage.get(field, 0) > 0
            )
            binary_recommended_score = sum(
                1 for field in recommended_fields
                if recommended_fields_coverage.get(field, 0) > 0
            )

            # Binary scores for augmented fields
            binary_required_augmented = sum(
                1 for field in required_augmented_fields
                if required_augmented_coverage.get(field, 0) > 0
            )
            binary_recommended_augmented = sum(
                1 for field in recommended_augmented_fields
                if recommended_augmented_coverage.get(field, 0) > 0
            )

            total_required_fields = len(required_fields)
            total_recommended_fields = len(recommended_fields)

            if total_required_fields > 0:
                percent_required_fields = round(
                    binary_required_score / total_required_fields, 2
                )
            else:
                percent_required_fields = 0

            if total_recommended_fields > 0:
                percent_recommended_fields = round(
                    binary_recommended_score / total_recommended_fields, 2
                )
            else:
                percent_recommended_fields = 0

            averages['binary_required_score'] = binary_required_score
            averages['binary_recommended_score'] = binary_recommended_score
            averages['binary_required_augmented'] = binary_required_augmented
            averages['binary_recommended_augmented'] = binary_recommended_augmented
            averages['percent_required_fields'] = percent_required_fields
            averages['percent_recommended_fields'] = percent_recommended_fields

            # Conditions of access uniformity
            averages['conditionsOfAccess'] = self.calculate_conditionsOfAccess_uniformity(
                datasource, collection)

            logging.info(f"Metadata Completeness calculated for {datasource}")
        else:
            averages = self._get_default_averages(
                required_fields, recommended_fields)
            logging.info(
                f"No results found for {datasource}. Returning default averages")

        # Save to cache if enabled
        if use_cache:
            self.save_to_cache(datasource, averages)

        try:
            client.close()
        except Exception:
            pass

        return averages

    def _get_default_averages(self, required_fields, recommended_fields):
        """Get default averages when no data is found."""
        return {
            'avg_augmented_recommended_ratio': 0,
            'avg_augmented_required_ratio': 0,
            'avg_recommended_score_ratio': 0,
            'avg_required_ratio': 0,
            'required_fields': {field: 0 for field in required_fields},
            'recommended_fields': {field: 0 for field in recommended_fields},
            'sum_required_coverage': 0,
            'sum_recommended_coverage': 0,
            'binary_required_score': 0,
            'binary_recommended_score': 0,
            'binary_required_augmented': 0,
            'binary_recommended_augmented': 0,
            'percent_required_fields': 0,
            'percent_recommended_fields': 0,
            'conditionsOfAccess': "Unknown"
        }

    def list_available_datasources(self):
        """List all available datasources in the database."""
        try:
            client = MongoClient(self.mongo_url)
            db = client["nde_hub_src"]
            collections = db.list_collection_names()
            client.close()
            return collections
        except Exception as e:
            logging.error(f"Failed to list datasources: {e}")
            return []

    def calculate_all_datasources(self, use_cache=False):
        """Calculate metadata compatibility for all available datasources."""
        datasources = self.list_available_datasources()
        results = {}

        for datasource in datasources:
            logging.info(f"Processing datasource: {datasource}")
            try:
                results[datasource] = self.calculate_metadata_compatibility_average(
                    datasource, use_cache=use_cache
                )
            except Exception as e:
                logging.error(f"Failed to process {datasource}: {e}")
                results[datasource] = {"error": str(e)}

        return results


def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(
        description="Calculate metadata compatibility averages for NDE datasources"
    )
    parser.add_argument(
        "--datasource",
        type=str,
        help="Specific datasource to analyze (if not provided, lists available datasources)"
    )
    parser.add_argument(
        "--mongo-url",
        type=str,
        default="mongodb://su11:27017/",
        help="MongoDB connection URL (default: mongodb://su11:27017/)"
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default="./cache",
        help="Cache directory (default: ./cache)"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable caching"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Calculate for all available datasources"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file to save results (JSON format)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available datasources and exit"
    )

    args = parser.parse_args()

    # Initialize calculator
    calculator = MetadataCompatibilityCalculator(
        mongo_url=args.mongo_url,
        cache_dir=args.cache_dir
    )

    # List datasources if requested
    if args.list:
        datasources = calculator.list_available_datasources()
        print("Available datasources:")
        for ds in sorted(datasources):
            print(f"  - {ds}")
        return

    # Calculate for all datasources
    if args.all:
        results = calculator.calculate_all_datasources(
            use_cache=not args.no_cache)

        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"Results saved to {args.output}")
        else:
            print(json.dumps(results, indent=2))
        return

    # Calculate for specific datasource
    if args.datasource:
        result = calculator.calculate_metadata_compatibility_average(
            args.datasource,
            use_cache=not args.no_cache
        )

        if args.output:
            with open(args.output, 'w') as f:
                json.dump({args.datasource: result}, f, indent=2)
            print(f"Results saved to {args.output}")
        else:
            print(json.dumps(result, indent=2))
        return

    # If no specific action, list available datasources
    datasources = calculator.list_available_datasources()
    print("Available datasources:")
    for ds in sorted(datasources):
        print(f"  - {ds}")
    print("\nUse --datasource <name> to calculate for a specific datasource")
    print("Use --all to calculate for all datasources")
    print("Use --help for more options")


if __name__ == "__main__":
    main()
