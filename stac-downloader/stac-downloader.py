#!/usr/bin/env python3
import os

import rasterio
from rasterio.features import bounds

# Standard library imports
from pathlib import Path
from json import load
import argparse

# Third party imports
from pystac_client import Client
from pyproj import Transformer


def parse() -> argparse.Namespace:
    description = "Downloader for STAC."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("-u", "--url", type=str, help="STAC endpoint.")
    parser.add_argument(
        "-c", "--collection", type=str, help="Collections of data to download from."
    )
    parser.add_argument(
        "-o", "--dir", type=Path, help="Output directory to download data into."
    )
    parser.add_argument(
        "-g", "--geojson", type=Path, help="Downloads only data within BBOX of geoJSON."
    )
    parser.add_argument("-b", "--bands", type=str, nargs="+", help="Bands to download.")

    return parser.parse_args()


def main():
    args = parse()

    geojson = args.geojson
    ls_url = args.url
    dst_dir = args.dir
    bands = args.bands
    collections = args.collection
    with open(geojson, "r") as fp:
        file_content = load(fp)
    geometry = file_content["features"][0]["geometry"]

    client = Client.open(url=ls_url)
    # Search items
    search = client.search(collections=collections, intersects=geometry)
    print(f"{search.matched()} items found")

    # Get items
    items = [i.to_dict()["assets"] for i in search.get_items()]

    dst_dir.mkdir(exist_ok=True, parents=True)
    links = []
    for item in items:
        if not all(x in list(item.keys()) for x in bands):
            continue
        for band in bands:
            links.append(item[str(band)]["href"])

    for geotiff_file in links:
        with rasterio.open(geotiff_file) as geo_fp:
            bbox = bounds(geometry)
            coord_transformer = Transformer.from_crs("epsg:4326", geo_fp.crs)
            # calculate pixels to be streamed in cog
            coord_upper_left = coord_transformer.transform(bbox[3], bbox[0])
            coord_lower_right = coord_transformer.transform(bbox[1], bbox[2])
            pixel_upper_left = geo_fp.index(coord_upper_left[0], coord_upper_left[1])
            pixel_lower_right = geo_fp.index(coord_lower_right[0], coord_lower_right[1])

            for pixel in pixel_upper_left + pixel_lower_right:
                # If the pixel value is below 0, that means that
                # the bounds are not inside of our available dataset.
                if pixel < 0:
                    print("Provided geometry extends available datafile.")
                    print("Provide a smaller area of interest to get a result.")
                    exit()

            # make http range request only for bytes in window
            window = rasterio.windows.Window.from_slices(
                (pixel_upper_left[0], pixel_lower_right[0]),
                (pixel_upper_left[1], pixel_lower_right[1]),
            )
            subset = geo_fp.read(1, window=window)
            height, width = subset.shape

            profile = geo_fp.profile.copy()
            profile["width"] = width
            profile["height"] = height
            transform = profile["transform"]
            new_transform = rasterio.Affine(
                transform[0],
                transform[1],
                coord_upper_left[0],
                transform[3],
                transform[4],
                coord_upper_left[1],
            )
            profile["transform"] = new_transform
            print(f"Writing {os.path.join(dst_dir, geotiff_file.split(r'/')[-1])}")
            with rasterio.open(
                os.path.join(dst_dir, geotiff_file.split("/")[-1]), "w", **profile
            ) as dst:
                dst.write(subset, 1)


if __name__ == "__main__":
    main()
