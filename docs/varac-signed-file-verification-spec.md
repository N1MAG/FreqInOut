# VarAC Signed File Verification Spec

Release target: 1.2.3

## Goal

Apply the existing Message Viewer signature and hash verification path to files received through VarAC and VarAC BBS workflows, matching the trust display already used for signed FLAmp files.

## Scope

- Reuse the existing GPG and hash verification settings, cache, worker, and UI state.
- Verify auth-capable files from `flamp`, `varac`, and `bbs` file origins.
- Treat `.k2s`, `.b2s`, `.sig`, `.asc`, and `.gpg` as authentication-capable file suffixes for those origins.
- Preserve existing settings keys such as `gpg_verify_flamp_k2s_enabled` and `hash_verify_flamp_k2s_enabled` for backward compatibility.

## Behavior

- VarAC and BBS `.k2s/.b2s` files are checked for embedded clearsigned content, detached signatures, checksum sidecars, and trusted local hash matches using the same verifier as FLAmp.
- VarAC and BBS signature sidecars are scanned and can verify against their payloads when the payload is present.
- Message rows receive the same auth state, auth detail, and trusted/untrusted indicators regardless of whether the file arrived through FLAmp, VarAC, or BBS.
- Non-auth file types such as `.txt`, `.html`, and images remain visible but are not signature/hash candidates.

## Non-Goals

- Do not change the stored settings schema.
- Do not alter VarAC ingest database schemas.
- Do not add VarAC-specific signing on compose; this change only verifies received/staged file artifacts visible in Message Viewer.

## Validation

- Unit coverage should assert that VarAC and BBS auth-capable suffixes are accepted while unrelated origins and non-auth suffixes remain excluded.
- Existing FLAmp verification tests should remain valid.
