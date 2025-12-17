
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECURITY_YAML = ROOT / "docs" / "internal" / "security_incidents.yaml"
CHANGELOG = ROOT / "CHANGELOG.md"

def main():
    if not SECURITY_YAML.exists():
        print("No security_incidents.yaml, skipping.")
        return
    data = yaml.safe_load(SECURITY_YAML.read_text()) or {}
    incidents = data.get("incidents", [])
    if not incidents:
        print("No incidents recorded.")
        return
    print(f"{len(incidents)} security incidents recorded. (Stub, not editing changelog.)")

if __name__ == "__main__":
    main()
