# Iamine

**Mina IA con tu PC mientras duermes**  
Red descentralizada de cómputo para Inteligencia Artificial.  
Cualquier laptop, PC gamer o celular idle aporta 2 núcleos + 2 GB RAM y recibe **$IAMINE** tokens.

**Whitepaper**: [docs/whitepaper.md](docs/whitepaper.md)  
**Sitio web (próximo)**: iamine.ai  
**Discord / Telegram**: (próximamente)  
**Testnet**: Q2 2026

## ¿Cómo funciona?
- Instalas el cliente ligero (Rust).  
- Eliges cuánto aportar (CPU/GPU/RAM).  
- La red divide preguntas de IA en micro-tareas → tus nodos procesan → ganas **$IAMINE**.  
- Blockchain (Solana) solo maneja pagos y reputación.  
- Todo P2P y verificado con Proof of Inference + ZK-SNARKs.

## Quick Start (Testnet)
```bash
git clone https://github.com/iamine-labs/iamine.git
cd iamine/client-rust
cargo run
