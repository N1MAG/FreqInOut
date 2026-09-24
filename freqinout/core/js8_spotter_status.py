from __future__ import annotations


MCF304_EXPECTED_RESPONSES = 8


def spotter_status_label(status_key: object) -> str:
    key = str(status_key or "").strip().lower()
    return {
        "red": "Not Functioning",
        "yellow": "Partially Functioning",
        "green": "Functioning",
    }.get(key, "Unknown")


def classify_mcf304_status(response_code: object) -> tuple[str, str]:
    digits = [ch for ch in str(response_code or "") if ch in "12345"]
    if not digits:
        return "unknown", "Unknown"
    if "3" in digits:
        return "red", spotter_status_label("red")
    if "2" in digits:
        return "yellow", spotter_status_label("yellow")
    if len(digits) >= MCF304_EXPECTED_RESPONSES and all(
        ch == "1" for ch in digits[:MCF304_EXPECTED_RESPONSES]
    ):
        return "green", spotter_status_label("green")
    return "unknown", "Unknown"


def classify_spotter_status(form_id: object, response_code: object) -> tuple[str, str, str]:
    fid = str(form_id or "").strip().upper().removeprefix("F!")
    response = str(response_code or "").strip().upper()
    if fid in {"104", "701C"}:
        status_key = {"1": "green", "2": "yellow", "3": "red"}.get(response[:1], "unknown")
        evidence = "Q1" if fid == "104" else "Q1 operational status"
        return status_key, spotter_status_label(status_key), evidence

    if fid == "301":
        codes = list(response)
        q2_map = {"1": "green", "2": "yellow", "3": "red", "4": "unknown"}
        q3_map = {"1": "green", "2": "yellow", "3": "yellow", "4": "red", "5": "unknown"}
        q4_q9_map = {"1": "green", "2": "yellow", "3": "red", "4": "unknown"}
        statuses: list[str] = []
        for idx in range(1, 9):
            code = codes[idx] if idx < len(codes) else ""
            if idx == 1:
                statuses.append(q2_map.get(code, "unknown"))
            elif idx == 2:
                statuses.append(q3_map.get(code, "unknown"))
            else:
                statuses.append(q4_q9_map.get(code, "unknown"))
        if "red" in statuses:
            key = "red"
        elif "yellow" in statuses:
            key = "yellow"
        elif statuses and all(value == "green" for value in statuses):
            key = "green"
        else:
            key = "unknown"
        return key, spotter_status_label(key), "Q2-Q9 aggregate"

    if fid == "304":
        key, label = classify_mcf304_status(response)
        return key, label, "Aggregate"

    if fid == "701B":
        codes = list(response)
        dimensions: list[str] = []
        if len(codes) > 2:
            dimensions.append({"1": "green", "2": "yellow", "3": "red"}.get(codes[2], "unknown"))
        if len(codes) > 4:
            dimensions.append({"1": "green", "2": "red", "3": "yellow"}.get(codes[4], "unknown"))
        for idx in (5, 6, 7):
            if len(codes) > idx:
                dimensions.append({"1": "green", "2": "yellow", "3": "red"}.get(codes[idx], "unknown"))
        known = [value for value in dimensions if value != "unknown"]
        if "red" in known:
            key = "red"
        elif "yellow" in known:
            key = "yellow"
        elif len(dimensions) == 5 and len(known) == 5 and all(value == "green" for value in known):
            key = "green"
        else:
            key = "unknown"
        return key, spotter_status_label(key), "Q3/Q5-Q8 aggregate"

    return "unknown", spotter_status_label("unknown"), ""
