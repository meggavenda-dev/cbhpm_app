
# =====================================================
# 3) CALCULAR  (UCO automático; mantém métrica UCO; checkboxes reativos)
# =====================================================
if aba_atual == "🧮 Calcular":
    lista_v = versoes()
    v_selecionada = st.sidebar.selectbox("Tabela Ativa", lista_v, key="v_global_calc") if lista_v else None

    if v_selecionada:
        st.subheader("🧮 Calculadora de Honorários CBHPM")

        st.markdown("""
            <style>
            [data-testid="stMetricValue"] { font-size: 1.8rem; color: #007bff; }
            .res-card {
                padding: 20px;
                border-radius: 10px;
                background-color: #f8f9fa;
                border-left: 5px solid #007bff;
                margin-bottom: 20px;
            }
            </style>
        """, unsafe_allow_html=True)

        # Valor monetário da UCO aplicado automaticamente (sem campo de input)
        UCO_VALOR_APLICADO = float(st.secrets.get("UCO_VALOR", 1.00))

        # --- Entradas reativas (sem formulário) ---
        col_cod, col_ajuste = st.columns([2, 1])
        cod_calc = col_cod.text_input("Código do Procedimento", placeholder="Ex: 10101012", key="in_calc")
        infla = col_ajuste.number_input("Ajuste Adicional (%)", 0.0, step=0.5, key="in_infla")

        st.write("**Aplicar ajuste em:** (marque para incluir no ajuste)")
        c_port, c_uco, c_fil = st.columns(3)
        aplicar_porte = c_port.checkbox("Porte", value=False, key="chk_aplicar_porte")
        aplicar_uco   = c_uco.checkbox("UCO",   value=False, key="chk_aplicar_uco")
        aplicar_filme = c_fil.checkbox("Filme", value=False, key="chk_aplicar_filme")

        filme_v = st.number_input("Valor Filme (R$)", 21.70, step=0.01, format="%.2f", key="in_filme_val")

        # --- Cálculo reativo: ocorre automaticamente ao mudar qualquer input ---
        if not cod_calc:
            st.info("Informe o **Código do Procedimento** para calcular.")
        else:
            res = buscar_dados(cod_calc, v_selecionada, "Código")
            if res.empty:
                st.error(f"O código '{cod_calc}' não foi encontrado na tabela {v_selecionada}.")
            else:
                p = res.iloc[0]

                # Fatores de ajuste por componente conforme checkboxes
                f_porte = (1 + infla/100) if (aplicar_porte and infla != 0) else 1.0
                f_uco   = (1 + infla/100) if (aplicar_uco   and infla != 0) else 1.0
                f_filme = (1 + infla/100) if (aplicar_filme and infla != 0) else 1.0

                # Cálculos
                porte_calc = p['porte'] * f_porte
                uco_calc   = p['uco']   * UCO_VALOR_APLICADO * f_uco
                filme_calc = p['filme'] * filme_v * f_filme
                total = porte_calc + uco_calc + filme_calc

                # Resultado visual
                st.markdown(f"""
                    <div class="res-card">
                        <small>Procedimento encontrado em <b>{v_selecionada}</b></small><br>
                        <span style='font-size: 1.2rem;'>{p['descricao']}</span>
                    </div>
                """, unsafe_allow_html=True)

                # Métricas: Porte, UCO, Filme e TOTAL (UCO mantida)
                c_porte, c_uco_box, c_filme, c_total = st.columns(4)
                c_porte.metric("Porte", f"R$ {porte_calc:,.2f}")
                c_uco_box.metric("UCO", f"R$ {uco_calc:,.2f}")
                c_filme.metric("Filme", f"R$ {filme_calc:,.2f}")

                # Delta do total aparece se houver ajuste (>0%) em ao menos um componente marcado
                houve_ajuste = (infla != 0) and (aplicar_porte or aplicar_uco or aplicar_filme)
                c_total.metric("TOTAL FINAL", f"R$ {total:,.2f}", delta=f"{infla:.2f}%" if houve_ajuste else None)

                # Feedback do escopo aplicado (opcional)
                if infla != 0:
                    componentes = []
                    if aplicar_porte: componentes.append("Porte")
                    if aplicar_uco:   componentes.append("UCO")
                    if aplicar_filme: componentes.append("Filme")
                    if componentes:
                        st.caption("ℹ️ Ajuste aplicado em: **" + ", ".join(componentes) + "**")
                    else:
                        st.caption("ℹ️ Ajuste adicional está **zerado** em todos os componentes (nenhum marcado).")
                st.divider()
    else:
        st.warning("Nenhuma versão disponível. Importe dados na aba '📥 Importar'.")
