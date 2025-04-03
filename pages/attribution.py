import streamlit as st
from eda import * 

def run_attribution():
    data_frames = load_data()
    orders_per_month_per_store = data_frames.get('orders_month_store')
    attribution_model = data_frames.get('attribution_model_90')
    attribution_cjm = data_frames.get('attribution_cjm')
    
    st.title("Attribution Model Analysis")
    with st.expander("✨ Overview"):
        st.write("""
        This app is divided into 3 pages:
        1. **EDA**  ℹ️ Use the sidebar navigation to select a dataset to explore
        2. **Outbound Sizing**
        3. **Attribution Model**
        """)

    attribution_model['MONTH'] = pd.to_datetime(attribution_model['MONTH'])
    attribution_model['ATTRIBUTED_ORDERS'] = attribution_model['ATTRIBUTED_ORDERS'].astype(int)
    attribution_model['ATTRIBUTED_REVENUE'] = attribution_model['ATTRIBUTED_REVENUE'].astype(int)
    

    # Plot Orders per month per Store
    st.subheader("Monthly Orders per Store Spike in November 2022") 
    fig = px.bar(orders_per_month_per_store,    
                 x='MONTH',
                y='ORDER_COUNT',
                color='STORE',
                title="Orders per Month per Store",
                labels={'ORDER_COUNT': 'Total Orders'},
                color_discrete_sequence=px.colors.qualitative.Pastel)
    st.plotly_chart(fig, use_container_width=True)
    st.write("The spike in November 2022 is suspected to be due to the enrichment of data thanks to Pixel tool.")
    st.divider()

    color_map = {'direct': px.colors.qualitative.Plotly[0],
                 'referral': px.colors.qualitative.Safe[5],
                 'google': px.colors.qualitative.Safe[3],
                 'attentive': px.colors.qualitative.Safe[1],
                 'facebook': px.colors.qualitative.Safe[7]}
    
    # Attribution source distribution
    st.markdown(
        '<h3> <span style="color: violet;">Referral</span> and <span style="color: #636EFA;">direct</span> are driving orders</h3>', 
        unsafe_allow_html=True
    )
    order_counts = attribution_model.groupby('ATTRIBUTION_SOURCE')['ATTRIBUTED_ORDERS'].sum().reset_index()
    total_orders = order_counts['ATTRIBUTED_ORDERS'].sum()
    order_counts['percentage'] = (order_counts['ATTRIBUTED_ORDERS'] / total_orders) * 100
    fig2 = px.pie(
        order_counts, 
        names='ATTRIBUTION_SOURCE',
        values=order_counts['percentage'],
        title="Attribution Source Distribution",
        color='ATTRIBUTION_SOURCE',
        color_discrete_map=color_map 
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Attribution sankey
    sources = list(attribution_model["ATTRIBUTION_SOURCE"].unique())
    nodes = sources + ["Purchase"]
    node_dict = {name: i for i, name in enumerate(nodes)} 

    links = {
        "source": [node_dict[source] for source in attribution_model["ATTRIBUTION_SOURCE"]],
        "target": [node_dict["Purchase"]] * len(attribution_model),
        "value": attribution_model["ATTRIBUTED_ORDERS"].tolist(),
        "color": [color_map[source] for source in attribution_model["ATTRIBUTION_SOURCE"]]
    }
    fig = go.Figure(go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=nodes,
            color=["lightgray" if node == "Purchase" else color_map.get(node, "lightgray") for node in nodes]
        ),
        link=dict(
            source=links["source"],
            target=links["target"],
            value=links["value"],
            color=links["color"]
        )
    ))
    st.plotly_chart(fig)
    st.divider()

    # Plot Orders or Revenue
    visualization_dimension = st.selectbox(
        'Choose a visualization dimension:',
        ['Orders', 'Revenue']
    )
    st.markdown('<h3> Holiday season <span style="color: violet;">referrals</span> outperformed <span style="color: #636EFA;">direct</span> attribution </h3>', 
        unsafe_allow_html=True
    )
    if visualization_dimension == 'Orders':
        fig = px.bar(attribution_model, 
                    x='MONTH', 
                    y='ATTRIBUTED_ORDERS', 
                    color='ATTRIBUTION_SOURCE',
                    barmode='stack', 
                    title="Attributed Orders by Source and Month",
                    color_discrete_map=color_map)
        fig.update_layout(
                xaxis=dict(
                    tickangle=-45,
                    rangeslider=dict(
                        visible=True,
                        thickness=0.05,
                    ),
                    type='date',
                    range=['2022-10-15','2023-06-15'],
                ),
                hovermode="x unified",
                height=600
            )
        st.plotly_chart(fig, use_container_width=True)


    elif visualization_dimension == 'Revenue':
        fig = px.bar(attribution_model, 
                    x='MONTH', 
                    y='ATTRIBUTED_REVENUE', 
                    color='ATTRIBUTION_SOURCE',
                    barmode='stack', 
                    title="Attributed Revenue by Source and Month",
                    color_discrete_map=color_map)
        fig.update_layout(
                xaxis=dict(
                    tickangle=-45,
                    rangeslider=dict(
                        visible=True,
                        thickness=0.05,
                    ),
                    type='date',
                ),
                hovermode="x unified",
                height=600
            )
        st.plotly_chart(fig)
    st.divider()

    # Refferal attribution focus
    PAGEREFERRER_contrib = attribution_model.groupby('PAGEREFERRER').agg(
        total_orders=('ATTRIBUTED_ORDERS', 'sum'),
        total_revenue=('ATTRIBUTED_REVENUE', 'sum')
    ).reset_index()

    PAGEREFERRER_contrib = PAGEREFERRER_contrib.sort_values(by=['total_orders','total_revenue'], ascending=False)

    toggle = st.radio(
        'Choose which metric to display for Page Referrer:',
        ('Orders by Page Referrer', 'Revenue by Page Referrer')
    )

    st.markdown('<h3> <span style="color: violet;">Referrals</span> seem to generate from within the store </h3>', 
        unsafe_allow_html=True
    )
    st.write('#### _(E.g. discount on 1st order from home page)_')
    if toggle == 'Orders by Page Referrer':
        fig = px.bar(PAGEREFERRER_contrib, 
                    x='PAGEREFERRER', 
                    y='total_orders', 
                    title="Total Revenue by Page <span style='color: violet;'>Referrer</span>",
                    labels={'total_orders': 'Total Orders'},
                    color_discrete_sequence=[px.colors.qualitative.Safe[5]])
        fig.update_layout(
                xaxis=dict(
                    tickangle=-45,
                    rangeslider=dict(
                        visible=True,
                        thickness=0.05,
                    ),
                    type='category',
                    range=[-1, 9.5],
                ),
                hovermode="x unified",
                height=800 
            )
        st.plotly_chart(fig, use_container_width=True)

    elif toggle == 'Revenue by Page Referrer':
        fig = px.bar(PAGEREFERRER_contrib, 
                    x='PAGEREFERRER', 
                    y='total_revenue', 
                    title="Total Revenue by Page <span style='color: violet;'>Referrer</span>",
                    labels={'total_revenue': 'Total Revenue'},
                    color_discrete_sequence=[px.colors.qualitative.Safe[5]])
        fig.update_layout(
                xaxis=dict(
                    tickangle=-45,
                    rangeslider=dict(
                        visible=True,
                        thickness=0.05,
                    ),
                    type='category',
                    range=[-1, 9.5],
                ),
                hovermode="x unified",
                height=800 
            )
        st.plotly_chart(fig, use_container_width=True)
    st.divider()

    # Multi-touch CJM
    # Most common first/last touchpoint?
    bar_data = attribution_cjm.groupby(['TOUCHPOINT_STEP', 'ATTRIBUTION_SOURCE'])['SHOPIFYORDERID'].nunique().reset_index()
    bar_data.columns = ['Touchpoint Step', 'Attribution Source', 'Order Count']

    fig_bar = px.bar(
        bar_data,
        x='Touchpoint Step',
        y='Order Count',
        color='Attribution Source',
        title="Orders by Touchpoint Step & Attribution Source",
        barmode='stack',
        color_discrete_map=color_map
    )
    fig_bar.update_layout(
                xaxis=dict(
                    tickangle=-45,
                    rangeslider=dict(
                        visible=True,
                        thickness=0.05,
                    ),
                    type='linear',
                    range=[0, 10.5],
                ),
                hovermode="x unified",
                height=600
            )

    st.markdown('<h3> <span style="color: #636EFA;">Direct</span> orders have more touchpoints, while <span style="color: violet;">referral</span> orders generally occur at initial visit </h3>', 
        unsafe_allow_html=True
    )
    st.plotly_chart(fig_bar)
    st.divider()

    # What touch drives conversion?
    attribution_cjm['TOUCHPOINT_STEP'] = pd.to_numeric(attribution_cjm['TOUCHPOINT_STEP'], errors='coerce')
    attribution_sources = attribution_cjm['ATTRIBUTION_SOURCE'].unique()
    selected_source = st.selectbox(
        'Select Attribution Sources',
        options=attribution_sources,
        index=0
    )

    filtered_data = attribution_cjm[
        attribution_cjm['SHOPIFYORDERID'].notnull() &
        attribution_cjm['ATTRIBUTION_SOURCE'].isin([selected_source]) &
        (attribution_cjm['TOUCHPOINT_STEP'] < 10) 
    ]
    funnel_data = filtered_data.groupby('TOUCHPOINT_STEP')['SHOPIFYORDERID'].nunique().reset_index()
    funnel_data.columns = ['Touchpoint Step', 'Order Count']
    funnel_color = color_map.get(selected_source, 'lightblue')
    fig_funnel = go.Figure(go.Funnel(
        y=funnel_data['Touchpoint Step'],
        x=funnel_data['Order Count'],
        textinfo="value+percent initial",
        marker=dict(color=funnel_color)
    ))

    if selected_source == 'direct':
        st.markdown('<h3> Opportunity to convert <span style="color: #636EFA;">direct</span> order at first visit instead of second </h3>', 
            unsafe_allow_html=True
        )
    elif selected_source == 'referral':
        st.markdown('<h3> <span style="color: violet;">Referral</span> orders have the best conversion </h3>', 
            unsafe_allow_html=True
        )
    elif selected_source == 'google':
        st.markdown('<h3> Orders from <span style="color: rgb(17, 119, 51);">Google</span> SEO seem to be the most interesting opportunity to invest in </h3>', 
            unsafe_allow_html=True
        )
    elif selected_source == 'attentive':
        st.markdown('<h3> <span style="color: rgb(204, 102, 119);">Attentive</span> orders take the longest to convert </h3>', 
            unsafe_allow_html=True
        )
    elif selected_source == 'facebook':
        st.markdown('<h3> Unlike <span style="color: rgb(17, 119, 51);">Google</span> orders, <span style="color: rgb(153,153,51);">Facebook</span> ads are not successful at producing orders </h3>', 
            unsafe_allow_html=True
        )
    st.plotly_chart(fig_funnel)
    st.divider()

    st.subheader("Key Take-aways")
    st.markdown("""
    - The analysis employs a Last-Touch Attribution Model with lookback window, helping stores understand which attribution sources are driving monthly orders. 
    - Referral traffic is the most significant source of orders, followed by Direct traffic.
    - Since we observe a decreasing trend in Direct traffic & a U-shaped trend in Referral traffic, a 90-day window length was chosen since referrals usually take a bit longer to convert. 
    However, a deeper analysis of the average time between touchpoints and order by attribution source could help optimize this window.
    - Further investigation into products and/or discounts could provide further insights into how paid marketing budgets should be allocated.
    """)