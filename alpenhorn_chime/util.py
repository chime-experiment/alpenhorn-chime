"""Alpenhorn CHIME utility functions."""

import chimedb.core as db
from alpenhorn.db import database_proxy as alpenhorn_proxy

from .inst import ArchiveInst
from .types import AcqType, FileType, AcqFileTypes


@db.atomic(read_write=True)
def update_types() -> None:
    """Set CHIME AcqType and FileType data.

    Adds or updates values in the AcqType, FileType, and AcqFileTypes
    tables to ensure type data is up-to-date.

    Some types which are no longer in production are added because there
    are references to them in the database.  FileTypes which are unused
    have no `pattern` specified, meaning Alpenhorn will never match them
    during an import.

    Does not remove unknown types.
    """

    # Initialise the proxy of the alpenhorn tables
    alpenhorn_proxy.initialize(db.proxy.obj)

    def typedict(id_, name, notes, info_class):
        return {
            "id": id_,
            "name": name,
            "info_class": info_class,
            "notes": notes,
        }

    def at(id_, name, notes, info_class):
        return typedict(id_, name, notes, info_class)

    def ft(id_, name, notes, info_class, pattern):
        td = typedict(id_, name, notes, info_class)
        td["pattern"] = pattern
        return td

    # List of known acqtypes
    acqtypes = [
        at(
            1,
            "corr",
            "Traditionally hand-tooled correlation products from a correlator.",
            "CorrAcqInfo",
        ),
        at(2, "hk", "Housekeeping data.", None),
        at(
            3,
            "rawadc",
            "Raw ADC data taken for testing the status of a correlator.",
            "RawadcAcqInfo",
        ),
        at(
            5,
            "weather",
            "Weather data scraped from the wview archive provided by DRAO.",
            None,
        ),
        at(
            6,
            "hkp",
            "New prometheus based scheme for recording housekeeping data.",
            None,
        ),
        at(
            7,
            "digitalgain",
            "FPGA digital gains from the F-Engine.",
            None,
        ),
        at(
            8,
            "gain",
            "Complex gains from the calibration broker.",
            None,
        ),
        at(
            9,
            "flaginput",
            "Good correlator input flags from the flagging broker.",
            None,
        ),
        at(
            11,
            "hfb",
            "21cm absorber (Hyper Fine Beam) data taken from a correlator.",
            "HFBAcqInfo",
        ),
    ]

    for acqtype in acqtypes:
        count = (
            AcqType.update(**acqtype).where(AcqType.name == acqtype["name"]).execute()
        )
        if count == 0:
            AcqType.insert(**acqtype).execute()

    # List of known filetypes
    filetypes = [
        ft(
            1,
            "corr",
            "Traditionally hand-tooled correlation products from a correlator.",
            "CorrFileInfo",
            r"(?P<chunk_number>[0-9]{8})_(?P<freq_number>[0-9]{4})\.h5",
        ),
        ft(
            2,
            "log",
            "A human-readable log file produced by acquisition software.",
            None,
            None,
        ),
        ft(3, "hk", "A housekeeping file.", None, None),
        ft(
            4,
            "atmel_id",
            "A short file listing the ATMEL ID's and human readable names in an HK acquisition.",
            None,
            None,
        ),
        ft(
            5,
            "rawadc",
            "Raw ADC data taken for testing the status of a correlator.",
            "RawadcFileInfo",
            r"[0-9]{6}\.h5",
        ),
        ft(6, "pdf", "A portable document file.", None, None),
        ft(
            10,
            "weather",
            "Weather data scraped from the wview archive provided by DRAO.",
            "WeatherFileInfo",
            r"(?P<date>20[1-9][0-9][01][0-9][0-3][0-9])\.h5",
        ),
        ft(11, "hkp", "Archive of the prometheus housekeeping data.", None, None),
        ft(
            12,
            "calibration",
            "Calibration data products.",
            "cal_info_class",
            r"[0-9]{8}\.h5",
        ),
        ft(
            14,
            "hfb",
            "21cm absorber (Hyper Fine Beam) data taken from a correlator.",
            "HFBFileInfo",
            r"hfb_(?P<chunk_number>[0-9]{8})_(?P<freq_number>[0-9]{4})\.h5",
        ),
    ]

    for filetype in filetypes:
        count = (
            FileType.update(**filetype)
            .where(FileType.name == filetype["name"])
            .execute()
        )
        if count == 0:
            FileType.insert(**filetype).execute()

    # The acqtype-to-filetype mapping
    for names in [
        ("corr", "corr"),
        ("rawadc", "rawadc"),
        ("weather", "weather"),
        ("digitalgain", "calibration"),
        ("gain", "calibration"),
        ("flaginput", "calibration"),
        ("hfb", "hfb"),
    ]:
        at = AcqType.get(name=names[0])
        ft = FileType.get(name=names[1])

        # First delete anything extra for this acq
        AcqFileTypes.delete().where(
            AcqFileTypes.acq_type == at, AcqFileTypes.file_type != ft
        ).execute()
        # Then add, if necessary
        try:
            AcqFileTypes.get(acq_type=at, file_type=ft)
        except AcqFileTypes.DoesNotExist:
            AcqFileTypes.insert(acq_type=at, file_type=ft).execute()


def update_inst() -> None:
    """Populate the ArchiveInst table."""

    inst = [
        (1, "stone"),
        (2, "abbot"),
        (3, "blanchard"),
        (4, "ben"),
        (5, "first9ucrate"),
        (6, "first9ucreat"),
        (7, "slot15"),
        (8, "slot16"),
        (9, "slot12"),
        (10, "slot4"),
        (11, "slot11"),
        (12, "slot5"),
        (13, "slot7"),
        (14, "slot10"),
        (15, "slot8"),
        (16, "slot9"),
        (17, "slot13"),
        (18, "slot6"),
        (19, "slot14"),
        (20, "slot3"),
        (21, "pathfinder"),
        (22, "slot2"),
        (23, "slot1"),
        (24, "mingun"),
        (25, "chime"),
        (26, "cnBg8"),
        (27, "csCg9"),
        (28, "csCg8"),
        (29, "csCg7"),
        (30, "csCg6"),
        (31, "csCg5"),
        (32, "csCg4"),
        (33, "csCg3"),
        (34, "csCg2"),
        (35, "csCg1"),
        (36, "csCg0"),
        (37, "csBg1"),
        (38, "csBg0"),
        (39, "csAg9"),
        (40, "csAg8"),
        (41, "csAg7"),
        (42, "csAg5"),
        (43, "csAg4"),
        (44, "csAg3"),
        (45, "csAg2"),
        (46, "cs9g9"),
        (47, "cs9g8"),
        (48, "cs9g7"),
        (49, "cs9g6"),
        (50, "cs9g5"),
        (51, "cs9g4"),
        (52, "cs9g3"),
        (53, "cs9g2"),
        (54, "cs8g9"),
        (55, "cs8g8"),
        (56, "cs8g7"),
        (57, "cs8g1"),
        (58, "cs8g0"),
        (59, "cs5g8"),
        (60, "cs5g7"),
        (61, "cs5g6"),
        (62, "cs5g5"),
        (63, "cs5g4"),
        (64, "cs5g3"),
        (65, "cs5g2"),
        (66, "cs4g9"),
        (67, "cs4g8"),
        (68, "cs4g6"),
        (69, "cs3g9"),
        (70, "cs3g8"),
        (71, "cs3g7"),
        (72, "cs3g6"),
        (73, "cs3g5"),
        (74, "cs3g4"),
        (75, "cs3g3"),
        (76, "cs3g2"),
        (77, "cs3g0"),
        (78, "cs2g9"),
        (79, "cs2g8"),
        (80, "cs2g7"),
        (81, "cs2g6"),
        (82, "cs2g2"),
        (83, "cs2g1"),
        (84, "cs1g2"),
        (85, "cs1g1"),
        (86, "cs1g0"),
        (87, "cs0g5"),
        (88, "cs0g3"),
        (89, "cs0g2"),
        (90, "cs0g1"),
        (91, "cs0g0"),
        (92, "cnDg9"),
        (93, "cnDg8"),
        (94, "cnDg7"),
        (95, "cnDg6"),
        (96, "cnDg5"),
        (97, "cnDg3"),
        (98, "cnDg2"),
        (99, "cnBg9"),
        (100, "cnBg7"),
        (101, "cnBg6"),
        (102, "cnBg5"),
        (103, "cnBg4"),
        (104, "cnBg3"),
        (105, "cnBg2"),
        (106, "cnBg1"),
        (107, "cnBg0"),
        (108, "cnAg9"),
        (109, "cnAg8"),
        (110, "cnAg7"),
        (111, "cnAg6"),
        (112, "cnAg5"),
        (113, "cnAg4"),
        (114, "cnAg3"),
        (115, "cnAg2"),
        (116, "cnAg1"),
        (117, "cn9g9"),
        (118, "cn9g8"),
        (119, "cn9g7"),
        (120, "cn9g6"),
        (121, "cn9g5"),
        (122, "cn9g3"),
        (123, "cn9g2"),
        (124, "cn9g1"),
        (125, "cn9g0"),
        (126, "cn8g9"),
        (127, "cn8g1"),
        (128, "cn8g0"),
        (129, "cn6g9"),
        (130, "cn6g8"),
        (131, "cn6g7"),
        (132, "cn6g6"),
        (133, "cn6g5"),
        (134, "cn6g4"),
        (135, "cn6g3"),
        (136, "cn6g2"),
        (137, "cn6g1"),
        (138, "cn5g0"),
        (139, "cn4g9"),
        (140, "cn4g8"),
        (141, "cn4g7"),
        (142, "cn4g6"),
        (143, "cn4g5"),
        (144, "cn4g4"),
        (145, "cn4g3"),
        (146, "cn4g1"),
        (147, "cn4g0"),
        (148, "cn3g9"),
        (149, "cn3g8"),
        (150, "cn3g7"),
        (151, "cn3g6"),
        (152, "cn3g5"),
        (153, "cn3g4"),
        (154, "cn3g3"),
        (155, "cn3g2"),
        (156, "cn3g0"),
        (157, "cn2g9"),
        (158, "cn2g8"),
        (159, "cn2g6"),
        (160, "cn2g3"),
        (161, "cn2g1"),
        (162, "cn2g0"),
        (163, "cn1g9"),
        (164, "cn1g8"),
        (165, "cn1g7"),
        (166, "cn1g5"),
        (167, "cn1g4"),
        (168, "cn1g3"),
        (169, "cn1g2"),
        (170, "cn1g1"),
        (171, "cn0g9"),
        (172, "cn0g7"),
        (173, "cn0g6"),
        (174, "cn0g5"),
        (175, "cn0g4"),
        (176, "cn0g3"),
        (177, "cn0g2"),
        (178, "cn0g1"),
        (179, "cn0g0"),
        (180, "cs4g7"),
        (181, "cs2g5"),
        (182, "cs2g3"),
        (183, "cs2g0"),
        (184, "cs1g4"),
        (185, "cs1g3"),
        (186, "cn8g8"),
        (187, "cn8g7"),
        (188, "cn8g6"),
        (189, "cn6g0"),
        (190, "cn5g8"),
        (191, "cn5g7"),
        (192, "cn5g4"),
        (193, "cn5g3"),
        (194, "cn5g2"),
        (195, "cn5g1"),
        (196, "cn3g1"),
        (197, "cn2g5"),
        (198, "cn2g4"),
        (199, "cn2g2"),
        (200, "cn1g6"),
        (201, "cn1g0"),
        (202, "cn5g5"),
        (203, "cn5g6"),
        (204, "csDg3"),
        (205, "cs8g5"),
        (206, "cs8g2"),
        (207, "cs6g6"),
        (208, "cs4g3"),
        (209, "cs4g2"),
        (210, "cs9g0"),
        (211, "csBg5"),
        (212, "csBg3"),
        (213, "cnCg9"),
        (214, "cnCg4"),
        (215, "cnCg3"),
        (216, "cs8g3"),
        (217, "cs6g9"),
        (218, "cs6g7"),
        (219, "cs6g1"),
        (220, "cs4g5"),
        (221, "cs4g4"),
        (222, "cs4g1"),
        (223, "cs4g0"),
        (224, "cs2g4"),
        (225, "cs1g6"),
        (226, "cs0g6"),
        (227, "cs9g1"),
        (228, "cs5g0"),
        (229, "csBg4"),
        (230, "csBg2"),
        (231, "cnDg4"),
        (232, "cnCg8"),
        (233, "cnCg6"),
        (234, "cnCg5"),
        (235, "cnCg2"),
        (236, "cnCg0"),
        (237, "cnAg0"),
        (238, "cs8g4"),
        (239, "cs6g8"),
        (240, "cs6g5"),
        (241, "cs6g4"),
        (242, "cs6g3"),
        (243, "cs6g2"),
        (244, "cs6g0"),
        (245, "cs5g1"),
        (246, "cnCg1"),
        (247, "csBg9"),
        (248, "csBg8"),
        (249, "csBg7"),
        (250, "csBg6"),
        (251, "cn8g4"),
        (252, "cn8g3"),
        (253, "csDg4"),
        (254, "cn8g2"),
        (255, "cnCg7"),
        (256, "csAg0"),
        (257, "csAg1"),
        (258, "cn4g2"),
        (259, "csDg2"),
        (260, "chimecal"),
        (261, "chimepb"),
        (262, "chimeN2"),
        (263, "chimetiming"),
        (264, "chime26m"),
        (265, "chimestack"),
        (266, "chime26mgated"),
        (267, "chimedroneN2"),
        (268, "chimedronegatedN2"),
        (269, "chimedronecal"),
    ]

    ArchiveInst.insert_many(inst, fields=[ArchiveInst.id, ArchiveInst.name]).execute()
