#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    Downloads the latest input artifacts from Weights & Biases, cleans it and saves locally and back to Weights & Biases

    input/args:  users can pass arguments
        input_artifact (str)
        output_artifact (str)
        output_type (str)
        output_description (str)
        min_price (float)
        max_price (float)

    output/return: saves output_artifact locally and Weights & Biases

    """

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info(
        f"Downloading input artifact {args.input_artifact}: latest version from to Weights & Biases")
    local_path = wandb.use_artifact(f"{args.input_artifact}:latest").file()
    input_artifact_df = pd.read_csv(local_path)

    logger.info(
        f"Cleaning {args.input_artifact} - removing outliers and changing last_review to datetime")
    min_price = float(args.min_price)
    max_price = float(args.max_price)
    idx = input_artifact_df['price'].between(min_price, max_price)

    logger.info(
        f"Cleaning {args.input_artifact} - changing last_review to datetime")
    input_artifact_df = input_artifact_df[idx].copy()
    input_artifact_df['last_review'] = pd.to_datetime(
        input_artifact_df['last_review'])

    if ('longitude' in input_artifact_df.columns) and ('latitude' in input_artifact_df.columns):
        idx = input_artifact_df['longitude'].between(-74.25, -73.50) & input_artifact_df['latitude'].between(40.5, 41.2)
        input_artifact_df = input_artifact_df[idx].copy()

    logger.info(f"Saving as {args.output_artifact} locally")
    input_artifact_df.to_csv(f"{args.output_artifact}", index=False)

    logger.info(f"Uploading {args.output_artifact} to Weights & Biases")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    logger.info(
        f"Success cleaning {args.input_artifact}:latest and uploading {args.output_artifact}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")
    parser.add_argument(
        "input_artifact",
        type=str,
        help="name of input artifact")
    parser.add_argument(
        "output_artifact",
        type=str,
        help="name of output artifact")
    parser.add_argument("output_type", type=str, help="type of output")
    parser.add_argument(
        "output_description",
        type=str,
        help="description of output")
    parser.add_argument("min_price", type=str, help="provide minimum price ")
    parser.add_argument("max_price", type=str, help="provide maximum price ")
    args = parser.parse_args()

    go(args)
