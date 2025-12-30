# =====================================================
# PAINEL DE CÁLCULO
# =====================================================
if menu == "🧮 Painel de Cálculo":
    st.subheader("Painel de Cálculo CBHPM")

    versoes = listar_versoes()

    if not versoes:
        st.warning("Nenhuma tabela importada.")
    else:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            versao = st.selectbox("Tabela CBHPM", versoes)

        with col2:
            codigo = st.text_input("Código TUSS")

        with col3:
            valor_filme = st.number_input(
                "Valor do Filme (m²)",
                min_value=0.0,
                value=21.70,
                step=0.01
            )

        with col4:
            inflator_pct = st.number_input(
                "Inflator (%)",
                min_value=0.0,
                value=0.0,
                step=0.5
            )

        if st.button("🧮 Calcular"):
            df = buscar_procedimento(codigo, None, versao)

            if df.empty:
                st.warning("Procedimento não encontrado.")
            else:
                proc = df.iloc[0]

                porte = to_float(proc['porte'])
                uco = to_float(proc['uco'])
                qtd_filme = to_float(proc['filme'])

                # 🔢 CÁLCULOS
                base = porte + uco
                inflator_valor = base * (inflator_pct / 100)
                total_filme = qtd_filme * valor_filme
                total = base + inflator_valor + total_filme

                st.info(f"**Descrição:** {proc['descricao']}")

                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Porte", f"R$ {porte:,.2f}")
                c2.metric("UCO", f"R$ {uco:,.2f}")
                c3.metric("Inflator", f"R$ {inflator_valor:,.2f}")
                c4.metric("Filme", f"R$ {total_filme:,.2f}")

                st.success(f"### 💰 Valor Total: R$ {total:,.2f}")
