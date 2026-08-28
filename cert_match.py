r"""Hvilken nøgle hører til hvilket certifikat?

    python cert_match.py C:\cert

Læser alle PEM-blokke i mappen (eller de filer der gives som argumenter) og
sammenligner RSA-modulus. Certifikat og nøgle med samme modulus hører sammen;
alt andet giver KEY_VALUES_MISMATCH i uvicorn.

Kun stdlib -- ingen openssl, ingen cryptography.
"""
import base64
import re
import sys
from pathlib import Path


def _pem_blocks(path):
    raw = path.read_bytes()
    for m in re.finditer(rb"-----BEGIN ([A-Z0-9 ]+)-----(.*?)-----END \1-----", raw, re.S):
        try:
            yield m.group(1).decode(), base64.b64decode(re.sub(rb"\s", b"", m.group(2)))
        except Exception:
            continue


def _ints(der, out, depth=0):
    """Gennemløb DER rekursivt og saml alle store INTEGER-værdier."""
    i = 0
    if depth > 8:
        return
    while i + 1 < len(der):
        tag = der[i]
        j = i + 1
        n = der[j]
        j += 1
        if n & 0x80:
            k = n & 0x7F
            if k == 0 or k > 4 or j + k > len(der):
                return
            n = int.from_bytes(der[j:j + k], "big")
            j += k
        if j + n > len(der):
            return
        body = der[j:j + n]
        if tag == 0x02 and len(body) >= 128:
            out.append(body.lstrip(b"\x00"))
        elif tag in (0x30, 0x31, 0xA0, 0xA3):
            _ints(body, out, depth + 1)
        elif tag == 0x03 and body[:1] == b"\x00":
            _ints(body[1:], out, depth + 1)
        elif tag == 0x04:
            _ints(body, out, depth + 1)
        i = j + n


def modulus(der):
    found = []
    _ints(der, found)
    return found[0] if found else None


def main(argv):
    mal = [Path(a) for a in argv] or [Path(r"C:\cert")]
    filer = []
    for m in mal:
        filer.extend(sorted(m.iterdir()) if m.is_dir() else [m])

    poster = []
    for f in filer:
        if not f.is_file():
            continue
        for nr, (slags, der) in enumerate(_pem_blocks(f)):
            mod = modulus(der)
            if mod:
                poster.append((f.name, nr, slags, mod))

    if not poster:
        print("Ingen RSA-nøgler eller -certifikater fundet.")
        return 1

    grupper = {}
    for navn, nr, slags, mod in poster:
        grupper.setdefault(mod, []).append(f"{navn} blok {nr} ({slags})")

    for n, (mod, medlemmer) in enumerate(grupper.items(), 1):
        har_noegle = any("PRIVATE KEY" in m for m in medlemmer)
        har_cert = any("CERTIFICATE" in m for m in medlemmer)
        status = "PAR OK" if (har_noegle and har_cert) else "alene"
        print(f"\n[{n}] {status}   modulus {mod[:6].hex()}...{mod[-4:].hex()}  ({len(mod)*8} bit)")
        for m in medlemmer:
            print(f"      {m}")
    print("\nEt gyldigt par skal have både PRIVATE KEY og CERTIFICATE i samme gruppe.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
