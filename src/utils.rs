use infusion::infusion::cid::cid_blake3;
use serde_json::Value;

/// Computes a canonical payload hash using BLAKE3 via Infusion.
/// This function is shared between the relay and can be replicated in clients.
pub fn compute_payload_hash(payload_json: &Option<Value>) -> String {
    let payload_bytes = if let Some(p) = payload_json.as_ref() {
        // Use a stable JSON representation. 
        // Note: For true canonicalization, one might use a specific library, 
        // but p.to_string() is a good start if clients do the same.
        p.to_string().into_bytes()
    } else {
        vec![]
    };
    let hash_bytes = cid_blake3(&payload_bytes);
    hex::encode(hash_bytes)
}
