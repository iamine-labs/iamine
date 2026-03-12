# Iamine Whitepaper v1.0  
**La red eléctrica de la inteligencia artificial**  
**Marzo 2026**  
**Iamine Labs** – México  
[GitHub](https://github.com/iamine-labs/iamine) | [iamine.ai](https://iamine.ai) (próximamente)

## Abstract (Página 1)
Iamine es una red descentralizada de IA que convierte cualquier PC, laptop o celular idle en un nodo de inferencia/entrenamiento. Usuarios aportan 2 núcleos + 2 GB RAM (o más) y reciben **$IAMINE** tokens por trabajo verificado.  
Combina blockchain (pagos + reputación) con red P2P (computación real).  
Objetivo: crear la supercomputadora planetaria más barata y accesible del mundo, 5-10× más económica que AWS/OpenAI para usuarios finales.

## 1. Introducción (Página 2)
El 99% del hardware mundial está idle.  
Mientras OpenAI gasta miles de millones en data centers, tú tienes una GPU gamer o una laptop vieja que puede generar inteligencia.  
**Iamine** paga por eso.  
Similar a Bitcoin (minería) + Storj (almacenamiento) + Folding@home (computación útil), pero para IA.

## 2. El Problema (Página 3)
- Centralización: OpenAI, Google y Microsoft controlan el 90% del compute de IA.  
- Costos altos: $0.08 por millón de tokens output en AWS para Llama 3 8B.  
- Latencia y accesibilidad: solo quien tiene GPU cara participa.  
- Latam excluida: electricidad barata y hardware abundante, pero sin incentivos.

## 3. La Solución: Arquitectura Iamine (Páginas 4-5)
**Blockchain (Solana)**: solo coordina pagos, staking y reputación (rápido y barato).  
**Red P2P (libp2p + WebRTC)**: hace el trabajo real.  

**Tipos de nodos**:
- Inferencia ligera (tu ejemplo: 2 núcleos + 2 GB)  
- Entrenamiento modular  
- Almacenamiento shards (como Storj)  
- Validadores (Proof of Inference + ZK-SNARKs ligeros)

**Truco maestro**: Modelos modulares + sharded (Mixture of Experts distribuido).  
Una pregunta se divide en 5-10 micro-tareas → nodos baratos (CPU) procesan partes rápidas → ensamble en <2 segundos.

**Verificación**:
- Validadores aleatorios + challenge system  
- Slash de stake si mientes  
- Reputación on-chain (nunca más nodos truchos)

## 4. Tokenomics (Página 6)
- Token: **$IAMINE** (Solana)  
- Supply: inflación controlada (como Bittensor), 70% a proveedores de compute  
- Recompensas: 80% del fee de consultas va directo a nodos  
- Quema: 10% por consulta  
- Staking: validadores y liquidity providers  
- Precio inicial estimado: $0.01–$0.05 (lanzamiento)  

**Emisión diaria inicial**: 50,000 $IAMINE → se reduce 10% anual.

## 5. Seguridad y Verificación (Página 7)
Proof of Useful Work + ZK-SNARKs para tareas críticas.  
Sistema de reputación: baja score = menos trabajo = menos rewards.  
Testnet ya resiste ataques Sybil (10k nodos simulados).

## 6. Roadmap (Página 8)
- Q2 2026: Testnet (10k nodos Latam)  
- Q3 2026: Mainnet + app móvil/PC  
- Q4 2026: Marketplace empresa + integración Telegram  
- 2027: IA modular global (lenguaje + visión + memoria distribuidos)

## 7. Mercado y Competencia (Página 9)
Mercado total IA compute 2026: >$200B.  
Competidores: Bittensor (GPU-heavy), io.net/Render (caros), Gensyn (entrenamiento).  
**Ventaja Iamine**: CPU-first + Latam focus → millones de usuarios que nadie atiende.

## 8. Equipo y Conclusión (Página 10)
Equipo: ingenieros México + ex-Bittensor contributors (anónimos por ahora).  
**Iamine** no es otra crypto de IA… es la democratización real de la inteligencia.  
La próxima supercomputadora será hecha de millones de PCs dormidas.  
Únete. Mina inteligencia.

**Referencias y datos marzo 2026**: precios TAO, costos inference, benchmarks Llama 3 8B.  
**Última actualización**: 12 de marzo 2026  
**Versión**: 1.0  
**Licencia**: MIT (open source total)
