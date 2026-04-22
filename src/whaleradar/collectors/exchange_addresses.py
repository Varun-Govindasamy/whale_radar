"""Known exchange deposit addresses for identifying on-chain exchange flows."""

from __future__ import annotations

# Curated list of known exchange hot/deposit wallet addresses.
# Sources: public blockchain explorers, exchange documentation.
# Format: address -> exchange name

BITCOIN_EXCHANGE_ADDRESSES: dict[str, str] = {
    # Binance
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo": "binance",
    "3JZq4atUahhuA9rLhXLMhhTo133J9rF97j": "binance",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s": "binance",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h": "binance",
    # Coinbase
    "3Kzh9qAqVWQhEsfQz7zEQL1EuSx5tyNLNS": "coinbase",
    "1FzWLkAahHooV3kzTgyx6qsXoRDrBsrACw": "coinbase",
    # Kraken
    "3FHNBLobJnbCTFTVakh5TXnU16E3SNtFsD": "kraken",
    "bc1qr4dl5wa7kl8yu792dceg9z5knl2gkn220lk7a9": "kraken",
    # Bitfinex
    "3JZdQVz8DitEMqRar5JMDgrGCX7jEuAMiR": "bitfinex",
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97": "bitfinex",
    # Gemini
    "3NhByuWxGr2fMhCK5dZTzRBbQQauGc5Cbm": "gemini",
    # OKX
    "3LYJfcfHPXYJreMsASk2jkn69LWEYKzexb": "okx",
}

ETHEREUM_EXCHANGE_ADDRESSES: dict[str, str] = {
    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": "binance",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": "binance",
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": "binance",
    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": "coinbase",
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": "coinbase",
    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": "kraken",
    "0x53d284357ec70ce289d6d64134dfac8e511c8a3d": "kraken",
    # Bitfinex
    "0x876eabf441b2ee5b5b0554fd502a8e0600950cfa": "bitfinex",
    "0x742d35cc6634c0532925a3b844bc9e7595f2bd1e": "bitfinex",
    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": "okx",
}


def lookup_exchange(address: str, blockchain: str) -> str | None:
    """Return exchange name if *address* is a known exchange wallet, else None."""
    address = address.lower().strip()
    if blockchain == "bitcoin":
        return BITCOIN_EXCHANGE_ADDRESSES.get(address)
    if blockchain == "ethereum":
        return ETHEREUM_EXCHANGE_ADDRESSES.get(address.lower())
    return None
