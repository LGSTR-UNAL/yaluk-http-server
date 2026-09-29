import io
import json
import logging
import os
import re
import zipfile
from functools import cache
from pathlib import Path
from typing import List
from uuid import uuid7

import boto3
import geopandas as gpd
import pandas as pd
from confluent_kafka import Producer
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from geopandas import GeoDataFrame
from matplotlib.figure import Figure
from shapely.geometry import LineString, Point, Polygon
from yaluk_network_lib import Network_lib as nl
import yaluk_network_lib.Lightning_lib as ls
import yaluk_network_lib.Project_lib as yp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


KAFKA_BOOTSTRAP_SERVERS = require_env("KAFKA_BOOTSTRAP_SERVERS")
ALL_REQUEST_TOPIC = require_env("ALL_REQUEST_TOPIC")
REQUEST_TOPIC = require_env("REQUEST_TOPIC")
S3_BUCKET = "yaluk-requests"

app = FastAPI()
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

STROKE_SCHEMA = {
    "type": "struct",
    "name": "nowcast-linet-stroke.Value",
    "fields": [
        {"type": "string", "optional": False, "field": "id"},
        {"type": "int32", "optional": False, "field": "Case"},
        {"type": "float", "optional": False, "field": "Ih1"},
        {"type": "float", "optional": False, "field": "Ih2"},
        {"type": "float", "optional": False, "field": "tao11"},
        {"type": "float", "optional": False, "field": "tao21"},
        {"type": "float", "optional": False, "field": "tao12"},
        {"type": "float", "optional": False, "field": "tao22"},
        {"type": "int8", "optional": False, "field": "n1"},
        {"type": "int8", "optional": False, "field": "n2"},
        {"type": "float", "optional": False, "field": "X0"},
        {"type": "float", "optional": False, "field": "Y0"},
        {"type": "boolean", "optional": False, "field": "delete_tmp"},
        {"type": "string", "optional": False, "field": "case"},
        {"type": "float", "optional": False, "field": "min_dist"},
        {"type": "int8", "optional": False, "field": "direct_strike"},
        {"type": "int32", "optional": False, "field": "Line_id"},
        {"type": "float", "optional": False, "field": "Rusck_maxV"},
        {"type": "string", "optional": False, "field": "surge_arresters"},
    ],
}

# Checked in this order, so e.g. "line_info.csv" is classified as "info", not "line".
FILE_KINDS = ["vertices", "info", "config", "transformers", "dps", "line"]
IGNORED_FILE_KINDS = {"info", "transformers", "dps"}
REQUIRED_FILE_KINDS = ["nodes", "line", "vertices", "config"]
NODE_COLUMNS = [
    "fid",
    "node_label",
    "RPAT",
    "configuration",
    "dps_label",
    "dps_phases",
    "dps_file",
    "trf_label",
    "trf_phases",
    "trf_type",
]
SAFE_NAME = re.compile(r"\w[\w .-]*")
# Same rule the worker applies before downloading the file from S3
PCH_FILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")

THIN_TOLERANCE = 50
MAX_DIST = 1  # meters
TOL = 0.5
DEFAULT_RUSCK_MAXV_THRESHOLD = 50.0

TRF_TYPES = pd.DataFrame(
    [
        ["Tr1", 340e-12, 215e-12, 744e-12, 10.0],
        ["Tr2", 440e-12, 115e-12, 644e-12, 20.0],
    ],
    columns=["id", "CHV", "CLV", "CHL", "ground"],
)


@cache
def get_configurations() -> dict:
    conductor = nl.Conductor()
    conductor.Radio = 0.0143
    conductor.Resistance = 0.2667
    conductor.Name = "Conductor 4/0"
    conductor.Description = "Conductor 4/0 AWG - catálogo CENTELSA"

    guard_cable = nl.Conductor()
    guard_cable.Radio = 0.0061
    guard_cable.Resistance = 0.5163
    guard_cable.Name = "Cable de guarda ACSR 110 KCMIL"
    guard_cable.Description = "Cable de guarda ACSR 110 KCMIL"

    horizontal_12m = [(conductor, 12, 2), (conductor, 12, 0), (conductor, 12, -2)]
    vertical = [(conductor, 11.9, 0), (conductor, 9.9, 0), (conductor, 7.9, 0)]
    vertical_desc = "Poste disposicion vertical"

    # key: (Name, Description, [(conductor, height, x), ...])
    specs = {
        "R-580": ("R-580", "Linea un solo conductor", horizontal_12m),
        "R-560": ("Mono LAR2", "Linea un solo conductor", horizontal_12m),
        "S-121": ("S-121", vertical_desc, vertical),
        "R-131": ("R-131", vertical_desc, horizontal_12m),
        "S-131": ("S-131", vertical_desc, horizontal_12m),
        "S-125": (
            "S-125",
            vertical_desc,
            [
                (conductor, 9.85, -0.9),
                (conductor, 11.25, 0.65),
                (conductor, 9.85, 0.9),
                (guard_cable, 12.9, 0),
            ],
        ),
        "R-114": (
            "R-114",
            vertical_desc,
            [(conductor, 11.5, 1.8), (conductor, 11.5, 0), (conductor, 11.5, -1.8)],
        ),
        "SH-226": ("SH-226", vertical_desc, horizontal_12m),
        "RH-231": ("RH-231", vertical_desc, horizontal_12m),
        "R-133": (
            "R-133",
            vertical_desc,
            [(conductor, 9.98, 2.38), (conductor, 9.98, 0), (conductor, 9.98, -2.38)],
        ),
        "TORRECILLA": ("TORRECILLA", vertical_desc, vertical),
        "R-550": ("RH-550", vertical_desc, horizontal_12m),
        "PORTICO": ("PORTICO", vertical_desc, horizontal_12m),
        "S-126": (
            "S-126",
            vertical_desc,
            [(conductor, 11.9, 2.53), (conductor, 11.9, 0), (conductor, 11.9, -2.53)],
        ),
        "R-126": (
            "R-126",
            vertical_desc,
            [(conductor, 11.9, 2.38), (conductor, 11.9, 0), (conductor, 11.9, -2.38)],
        ),
        "C345": (
            "",
            "Poste en H 34,5 kV",
            [
                (conductor, 12, -2),
                (conductor, 12, 0),
                (conductor, 12, 2),
                (guard_cable, 14, -1),
                (guard_cable, 14, 1),
            ],
        ),
        "C144": (
            "",
            "Poste vertical 14,4 kV",
            [
                (conductor, 10.0, -1),
                (conductor, 10.0, 1),
                (conductor, 10.0, 0),
                (guard_cable, 12, 0),
            ],
        ),
        "C230": (
            "",
            "Poste vertical 230 kV",
            [
                (conductor, 10.0, -1),
                (conductor, 10.0, 1),
                (conductor, 10.0, 0),
                (guard_cable, 12, 0),
            ],
        ),
    }

    configurations = {}
    for key, (name, description, layout) in specs.items():
        configuration = nl.Configuration_Type()
        configuration.id = key
        configuration.Name = name
        configuration.Description = description
        for cond, height, x in layout:
            configuration.Add_conductor(cond, height, x)
        configuration.gen_parameters()
        configurations[key] = configuration
    return configurations


def build_project_settings():
    simulation_settings_df = pd.DataFrame(
        [[15.0, 0, 5000.0, False, False, False, False]],
        columns=[
            "Dx",
            "Typewaveform",
            "MaxDistCamp",
            "Print_field",
            "Print_field_info",
            "Read_field",
            "Status_profile",
        ],
    )
    channel_settings_df = pd.DataFrame(
        [[2e8, 2e3, 8000]], columns=["Velocity", "Atenuation_const", "Heigth"]
    )
    soil_settings_df = pd.DataFrame(
        [[1e-3, 10.0, False]], columns=["Conductivity", "Permitivity", "Perfectsoil"]
    )
    atp_settings_df = pd.DataFrame(
        [[999999, 1, 1, 1, 1, 1, 0, 0]],
        columns=[
            "printfreq",
            "plotfreq",
            "pl4out",
            "NetwrkOut",
            "SSPhasor",
            "MaxMinOut",
            "MemSave",
            "ExtrPinOut",
        ],
    )
    return yp.setup_settings(
        simulation_settings_df, channel_settings_df, soil_settings_df, atp_settings_df
    )


def classify_file(filename: str) -> str:
    tokens = re.split(r"[^A-Za-z0-9]+", Path(filename).stem)
    for kind in FILE_KINDS:
        if any(token.startswith(kind) for token in tokens):
            return kind
    return "nodes"


def require_columns(df: pd.DataFrame, columns: List[str], source: str):
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise HTTPException(400, f"{source} is missing columns: {', '.join(missing)}")


def config_value(line_data: pd.DataFrame, key: str, default=None):
    value = line_data["value"].get(key)
    if value is None or pd.isna(value) or str(value).strip() == "":
        if default is None:
            raise HTTPException(400, f"Missing '{key}' in config file")
        return default
    return value


def phase_mask(value, source: str) -> int:
    # yaluk_network_lib expects a 3-digit 0/1 mask (e.g. 111 = three-phase) as a
    # Python int; CSV columns with empty cells are read as floats (111.0).
    try:
        mask = int(float(value))
        valid = float(value) == mask and 0 < mask and set(str(mask)) <= {"0", "1"}
    except (TypeError, ValueError):
        valid = False
    if not valid or mask > 111:
        raise HTTPException(
            400, f"Invalid {source}: '{value}' (use 100, 10, 1, 110, 101, 11 or 111)"
        )
    return mask


def exceeds(value, threshold: float) -> bool:
    try:
        return float(value) > threshold
    except (TypeError, ValueError):
        return False


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Kafka delivery to {msg.topic()} failed: {err}")


def produce(topic: str, value: str):
    while True:
        try:
            producer.produce(topic=topic, value=value, on_delivery=delivery_report)
            producer.poll(0)
            return
        except BufferError:
            producer.poll(0.1)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        request=request, name="index.html", context={"request": request}
    )


@app.post("/procesar/")
def procesar_archivos(files: List[UploadFile] = File(...)):
    geodataframe: GeoDataFrame | None = None
    line: GeoDataFrame | None = None
    original_vertices: pd.DataFrame | None = None
    line_data: pd.DataFrame | None = None
    seen_kinds = set()

    for file in files:
        if not file.filename:
            continue
        kind = classify_file(file.filename)
        if kind in IGNORED_FILE_KINDS:
            continue
        if kind in seen_kinds:
            raise HTTPException(400, f"More than one '{kind}' file was uploaded")
        seen_kinds.add(kind)
        contenido = file.file.read()

        if kind == "nodes":
            if Path(file.filename).suffix.lower() == ".csv":
                df = pd.read_csv(io.BytesIO(contenido))
                require_columns(df, ["X", "Y"], file.filename)
                geodataframe = gpd.GeoDataFrame(
                    df, geometry=gpd.points_from_xy(df.X, df.Y), crs="EPSG:3118"
                )
            else:
                geodataframe = gpd.read_file(io.BytesIO(contenido))
            require_columns(geodataframe, NODE_COLUMNS, file.filename)
            geodataframe["fid"] = geodataframe["fid"].astype(float)
            geodataframe["node_label"] = geodataframe["node_label"].astype(str)
            geodataframe.sort_values(by="fid", inplace=True)
        elif kind == "line":
            raw_line = gpd.read_file(io.BytesIO(contenido))
            line = gpd.GeoDataFrame(
                geometry=[raw_line.geometry.force_2d().union_all()], crs=raw_line.crs
            )
        elif kind == "vertices":
            original_vertices = pd.read_csv(io.BytesIO(contenido), sep=",")
            require_columns(original_vertices, ["from_node", "to_node"], file.filename)
            original_vertices = original_vertices[["from_node", "to_node"]].astype(str)
        elif kind == "config":
            line_data = pd.read_csv(io.BytesIO(contenido), sep=",", index_col=0)
            require_columns(line_data, ["value"], file.filename)

    missing = [k for k in REQUIRED_FILE_KINDS if k not in seen_kinds]
    if missing:
        raise HTTPException(400, f"Missing required files: {', '.join(missing)}")
    assert geodataframe is not None and line is not None
    assert original_vertices is not None and line_data is not None

    line_name = str(config_value(line_data, "name")).strip()
    if not SAFE_NAME.fullmatch(line_name) or ".." in line_name:
        raise HTTPException(400, f"Invalid line name '{line_name}'")
    simplified = str(config_value(line_data, "simplified")).strip() == "True"
    try:
        num_events = int(config_value(line_data, "events"))
        rusck_maxv_threshold = float(
            config_value(
                line_data, "rusck_maxv_threshold", DEFAULT_RUSCK_MAXV_THRESHOLD
            )
        )
    except ValueError as e:
        raise HTTPException(400, f"Invalid numeric value in config file: {e}")

    new_line = line.copy()
    if not simplified:
        new_line.loc[0, "geometry"] = thin_geometry(
            line.loc[0, "geometry"], THIN_TOLERANCE
        )
    geom_simplified = new_line.loc[0, "geometry"]

    vertices = extract_vertices(geom_simplified)
    vertices_gdf = gpd.GeoDataFrame(geometry=vertices, crs=geodataframe.geometry.crs)

    fig = Figure(figsize=(10, 10))
    ax = fig.subplots()
    geodataframe.plot(ax=ax, color="red")
    line.plot(ax=ax, color="red")
    new_line.plot(ax=ax, color="blue")
    vertices_gdf.plot(ax=ax, color="green", markersize=50)
    vertices_image_buffer = io.BytesIO()
    fig.savefig(vertices_image_buffer, dpi=300)

    matched = gpd.sjoin_nearest(
        vertices_gdf,
        geodataframe[["node_label", "geometry"]],
        how="left",
        distance_col="dist",
    )
    matched.loc[matched["dist"] > MAX_DIST, "node_label"] = None

    joined = gpd.sjoin_nearest(
        geodataframe,
        matched[["node_label", "geometry"]],
        how="left",
        distance_col="dist",
    )
    joined = joined.drop_duplicates(subset="node_label_left")

    geodataframe["is_in_simplified"] = (joined["dist"] <= TOL).reindex(
        geodataframe.index, fill_value=False
    )
    geodataframe["closest_simplified_name"] = joined["node_label_right"]

    name_map = geodataframe.set_index("node_label")["closest_simplified_name"].to_dict()
    for key, value in name_map.items():
        if not value or pd.isna(value):
            name_map[key] = key

    remapped_segments = original_vertices.replace(name_map).values.tolist()
    unique_segments = []
    seen = set()
    for a, b in remapped_segments:
        key = frozenset((a, b))
        if a != b and key not in seen:
            seen.add(key)
            unique_segments.append([a, b])

    surge_arresters = geodataframe[geodataframe["dps_file"].notnull()].copy()[
        ["dps_label", "node_label", "dps_phases", "dps_file"]
    ]
    surge_arresters["node_label"] = [
        name_map.get(vname, vname) for vname in surge_arresters["node_label"]
    ]
    surge_arresters.rename(
        columns={
            "dps_label": "label",
            "dps_phases": "phases",
            "dps_file": "file",
            "node_label": "node",
        },
        inplace=True,
    )
    for sa in surge_arresters.itertuples():
        if not PCH_FILE_NAME.fullmatch(str(sa.file)) or ".." in str(sa.file):
            raise HTTPException(
                400,
                f"Invalid dps_file '{sa.file}' for surge arrester '{sa.label}': "
                "use only the file name as stored in S3 (e.g. 15kV-3ph.pch)",
            )
    surge_arresters["phases"] = [
        phase_mask(p, f"dps_phases of surge arrester '{label}'")
        for p, label in zip(surge_arresters["phases"], surge_arresters["label"])
    ]

    transformers = geodataframe[geodataframe["trf_type"].notnull()].copy()[
        ["trf_label", "node_label", "trf_phases", "trf_type"]
    ]
    transformers["node_label"] = [
        name_map.get(vname, vname) for vname in transformers["node_label"]
    ]
    transformers.rename(
        columns={
            "trf_label": "label",
            "node_label": "from_node",
            "trf_phases": "phases",
        },
        inplace=True,
    )
    transformers["phases"] = [
        phase_mask(p, f"trf_phases of transformer '{label}'")
        for p, label in zip(transformers["phases"], transformers["label"])
    ]

    nodes = geodataframe[geodataframe["is_in_simplified"]][["node_label", "geometry"]]
    nodes = nodes.drop_duplicates(subset="node_label").reset_index(drop=True)
    nodes.index = nodes.index + 1
    nodes["label"] = nodes["node_label"]
    nodes["x"] = nodes.geometry.x
    nodes["y"] = nodes.geometry.y
    nodes["id"] = nodes.index
    nodes = nodes[["id", "label", "x", "y"]]
    geodataframe.rename(columns={"node_label": "label"}, inplace=True)
    node_attributes = geodataframe[["label", "RPAT", "configuration"]].drop_duplicates(
        subset="label"
    )
    nodes = nodes.merge(node_attributes, on="label", how="left")

    info_columns = ["label", "is_in_simplified", "closest_simplified_name"]
    info_df = geodataframe[info_columns].copy()
    info_df["label"] = info_df["closest_simplified_name"]
    info_df = info_df.merge(
        nodes.drop(columns=["RPAT", "configuration"]), on="label", how="left"
    )

    lines = pd.DataFrame(unique_segments, columns=["from_node", "to_node"])
    lines.index = lines.index + 1
    lines["id"] = lines.index
    lines = lines[["id", "from_node", "to_node"]]

    try:
        net = network_setting(
            nodes,
            lines,
            transformers,
            surge_arresters,
            TRF_TYPES,
            line_name,
            get_configurations(),
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    proj_settings = build_project_settings()
    yaluk_ini, yaluk_status = yp.write_yaluk_init_file(proj_settings, net, buffer=True)
    misc_file = yp.write_misc_file(proj_settings, net, buffer=True)

    lines_buffer = net.MakeLineFiles()

    lig_stat = ls.Lightning_stat(net, num_events, "ProbDistribution")
    all_lightning_buffer = lig_stat.Create_csvBuffer()
    all_lightning_buffer.seek(0)
    all_lightning_df = pd.read_csv(all_lightning_buffer, sep=";")
    numeric_columns = all_lightning_df.select_dtypes("number").columns
    text_columns = all_lightning_df.columns.difference(numeric_columns)
    all_lightning_df[text_columns] = all_lightning_df[text_columns].fillna(
        "Not defined"
    )
    # Missing numeric values are sent as JSON null instead of NaN (invalid JSON)
    all_lightning_df = all_lightning_df.astype(object).where(
        all_lightning_df.notna(), None
    )

    with zipfile.ZipFile(
        lines_buffer, mode="a", compression=zipfile.ZIP_DEFLATED
    ) as zf:
        zf.writestr("yaluk.ini", yaluk_ini.getvalue())
        zf.writestr("yaluk_status.ini", yaluk_status.getvalue())
        zf.writestr("CaseFiles/miscelaneo.txt", misc_file.getvalue())
        zf.writestr(f"{line_name}_all.csv", all_lightning_buffer.getvalue())

    logging.info(f"Uploading cases for {line_name} to S3 bucket '{S3_BUCKET}'")
    for index, ds in enumerate(lig_stat.Lightnings):
        ds_atp_buffer: io.BytesIO = net.MakeATPBuffer(None, ds)
        strike_buffer: io.BytesIO = ds.MakeLightningBuffer(index)
        strike_buffer.seek(0)
        s3.upload_fileobj(
            strike_buffer, S3_BUCKET, f"{line_name}/corr_{(ds.case+1):05d}.txt"
        )
        ds_atp_buffer.seek(0)
        s3.upload_fileobj(ds_atp_buffer, S3_BUCKET, f"{line_name}_{ds.case+1}.atp")

    net.MakeLccFile()
    lines_buffer = net.get_lcc_lib_zipbuffer(line_name, lines_buffer)
    lines_buffer.seek(0)

    logging.info(f"Uploading {line_name}.zip to S3 bucket '{S3_BUCKET}'")
    s3.upload_fileobj(lines_buffer, S3_BUCKET, f"{line_name}.zip")

    surge_arresters_json = json.dumps(surge_arresters["file"].unique().tolist())
    requests_published = 0
    for record in all_lightning_df.to_dict(orient="records"):
        record["id"] = str(uuid7())
        record["delete_tmp"] = True
        record["case"] = line_name
        record["surge_arresters"] = surge_arresters_json
        message = json.dumps({"schema": STROKE_SCHEMA, "payload": record})
        produce(ALL_REQUEST_TOPIC, message)
        if exceeds(record.get("Rusck_maxV"), rusck_maxv_threshold):
            produce(REQUEST_TOPIC, message)
            requests_published += 1
        logging.debug(message)

    pending = producer.flush(60)
    if pending:
        logging.error(f"{pending} Kafka messages were not delivered for {line_name}")
    logging.info(
        f"Published {len(all_lightning_df)} records to {ALL_REQUEST_TOPIC} and "
        f"{requests_published} to {REQUEST_TOPIC} for {line_name}"
    )

    response = io.BytesIO()
    with zipfile.ZipFile(response, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{line_name}_vertices.png", vertices_image_buffer.getvalue())
        zf.writestr(f"{line_name}_info.csv", info_df.to_csv(index=False))
    response.seek(0)
    return StreamingResponse(
        response,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=resultado.zip"},
    )


def extract_vertices(geom):
    if geom.geom_type == "Point":
        return [geom]

    if geom.geom_type in ["LineString", "LinearRing"]:
        return [Point(xy) for xy in geom.coords]

    if geom.geom_type == "Polygon":
        points = [Point(xy) for xy in geom.exterior.coords]
        for ring in geom.interiors:
            points.extend(Point(xy) for xy in ring.coords)
        return points

    if geom.geom_type.startswith("Multi"):
        points = []
        for part in geom.geoms:
            points.extend(extract_vertices(part))
        return points

    return []


def thin_coords_by_distance(coords, min_dist):
    new_coords = [coords[0]]

    for pt in coords[1:]:
        last_pt = new_coords[-1]
        dx = pt[0] - last_pt[0]
        dy = pt[1] - last_pt[1]
        dist = (dx**2 + dy**2) ** 0.5

        if dist >= min_dist:
            new_coords.append(pt)

    # Always keep last point (replacing the last kept one, but never the first)
    if new_coords[-1] != coords[-1]:
        if len(new_coords) > 1:
            del new_coords[-1]
        new_coords.append(coords[-1])

    return new_coords


def thin_linestring_by_distance(line, min_dist):
    coords = list(line.coords)
    if len(coords) <= 2:
        return line

    new_coords = thin_coords_by_distance(coords, min_dist)
    if len(new_coords) < 2:
        return line
    return LineString(new_coords)


def thin_ring_by_distance(ring, min_dist):
    coords = list(ring.coords)
    new_coords = thin_coords_by_distance(coords, min_dist)
    # A valid ring needs at least 3 distinct points plus the closing one
    return new_coords if len(new_coords) >= 4 else coords


def thin_polygon_by_distance(polygon, min_dist):
    return Polygon(
        thin_ring_by_distance(polygon.exterior, min_dist),
        [thin_ring_by_distance(ring, min_dist) for ring in polygon.interiors],
    )


def thin_geometry(geom, min_dist):
    if geom.geom_type == "LineString":
        return thin_linestring_by_distance(geom, min_dist)

    if geom.geom_type == "Polygon":
        return thin_polygon_by_distance(geom, min_dist)

    if geom.geom_type.startswith("Multi"):
        return geom.__class__([thin_geometry(g, min_dist) for g in geom.geoms])

    return geom


def network_setting(
    Nodes: pd.DataFrame,
    Lines: pd.DataFrame,
    Transformers: pd.DataFrame,
    Surge_Arresters: pd.DataFrame,
    Trftype: pd.DataFrame,
    line_name: str = "linea_1",
    Configurations: dict | None = None,
):
    if Configurations is None:
        Configurations = get_configurations()
    # Create a new network
    len_nodes = len(Nodes)
    len_lines = len(Lines)
    len_transformers = len(Transformers)
    len_surge_arresters = len(Surge_Arresters)
    if len_nodes == 0:
        raise ValueError("No nodes defined in the network.")
    elif len_lines == 0:
        raise ValueError("No lines defined in the network.")
    logging.info(
        f"Creating network with {len_nodes} nodes, {len_lines} lines, {len_transformers} transformers, and {len_surge_arresters} surge arresters."
    )
    # Initial location for coordinate conversion
    # Init_lat_lon= nl.PointLatLng(Nodes.loc[0,'x'], Nodes.loc[0,'y'])
    initxy = nl.PointXY(Nodes.iloc[0]["x"], Nodes.iloc[0]["y"])

    net = nl.Network("uid_01", line_name)
    net.InitXY = initxy

    # Dfining Configurations, it culd be severalconfigurations with different number of conductors and geometries, each line must be assigned to one of the configurations

    for n in Nodes.itertuples():
        net.AddNode(
            n.x,
            n.y,
            Nodeidx=n.id,
            ctype="abs",
            label=n.label,
            RPAT=n.RPAT,
            configuration=n.configuration,
        )  # You can use also ctype="rel" if you want to add nodes using coordinates relative to the initial location
    # Add lines
    for l in Lines.itertuples():
        from_node = net.Nodes.GetByName(l.from_node)
        to_node = net.Nodes.GetByName(l.to_node)
        if from_node is None or to_node is None:
            missing = l.from_node if from_node is None else l.to_node
            raise ValueError(f"Line {l.id}: node '{missing}' not found in network.")
        configuration = Configurations.get(from_node.Configuration)
        if configuration is None:
            raise ValueError(
                f"Unknown configuration '{from_node.Configuration}' at node '{l.from_node}'."
            )
        net.AddLine(from_node, to_node, configuration)
    for li in net.Lines:
        li.isFDTD = True
    # Add transformers
    for t in Transformers.itertuples():
        node_trf = t.from_node
        trf_types = Trftype.loc[Trftype["id"] == t.trf_type]
        if trf_types.empty:
            raise ValueError(
                f"Unknown transformer type '{t.trf_type}' for transformer '{t.label}'."
            )
        trf_type = trf_types.iloc[0]
        net.AddTransf(
            label=t.label,
            CHV=float(trf_type.CHV),
            CLV=float(trf_type.CLV),
            CHL=float(trf_type.CHL),
            ground=float(trf_type.ground),
            NHV=node_trf,
            phases=t.phases,
        )
    for sa in Surge_Arresters.itertuples():
        node_sa = net.Nodes.GetByName(sa.node)
        if node_sa is None:
            raise ValueError(
                f"Surge arrester '{sa.label}': node '{sa.node}' not found in network."
            )
        net.AddSurgeArrester(ni=node_sa, pch_file=sa.file, Phase_connect=sa.phases)
    return net
