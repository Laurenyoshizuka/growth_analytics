import streamlit as st
from eda import * 
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

def run_outbound_sizing():
    data_frames = load_data()
    outbound_data = data_frames.get('outbound')
    outbound_data = clean_df(outbound_data)
    market_data = data_frames.get('market')
    market_data = clean_df(market_data)

    st.title("Outbound Sizing Analysis")
    with st.expander("✨ Overview"):
        st.write("""
        This app is divided into 3 pages:
        1. **EDA**  ℹ️ Use the sidebar navigation to select a dataset to explore
        2. **Outbound Sizing**
        3. **Attribution Model**
        """)

    st.subheader("NEW_ARR_FROM_OB_ALL_TIME as a Metric to Assess Campaign Efficacy")

    # Outbound effectiveness
    outbound_data_sorted = outbound_data.sort_values(by=['NEW_ARR_FROM_OB_ALL_TIME'], ascending=False)
    outbound_data_sorted['CUMULATIVE_NEW_ARR'] = outbound_data_sorted['NEW_ARR_FROM_OB_ALL_TIME'].cumsum()
    total_new_arr = outbound_data_sorted['NEW_ARR_FROM_OB_ALL_TIME'].sum()
    threshold = 0.8 * total_new_arr
    outbound_data_sorted['COLOR_LABEL'] = outbound_data_sorted['CUMULATIVE_NEW_ARR'].apply(
        lambda x: 'Top 80%' if x <= threshold else 'Other'
    )
    num_campaigns_top_80 = (outbound_data_sorted['CUMULATIVE_NEW_ARR'] <= threshold).sum()
    percentage_top_80 = (num_campaigns_top_80 / len(outbound_data_sorted)) * 100

    fig1 = px.bar(outbound_data_sorted, 
                  x='CAMPAIGN_GROUP', 
                  y='NEW_ARR_FROM_OB_ALL_TIME', 
                  title=f"{percentage_top_80:.0f}% of Campaigns Account for 80% of Total New ARR",
                  labels={'NEW_ARR_FROM_OB_ALL_TIME': 'New ARR ($)', 'CAMPAIGN_GROUP': 'Campaign'},
                  color='COLOR_LABEL', 
                  color_discrete_map={'Top 80%': 'blue', 'Other': 'lightgray'})
    st.plotly_chart(fig1, use_container_width=True)
    st.markdown("""
                - **The top 6 campaigns include: :blue[GA4, GPT V3-CAPI, Loom, Klaviyo flows enrich, Ask Polar Lite, and GPT V4 (GPT-4o)].**
                - Using a proxy for the CAC : CLV ratio, the North Star metric for outbound campaign efficacy is NEW_ARR_FROM_OB_ALL_TIME, 
                since it shows the actual revenue impact of outbound campaigns.
    """)
    st.divider()

    # Outbound opportunity size
    st.subheader("The United States is key in generating New ARR from scaling to the TAM")
    shopify_data = market_data[(market_data['PLATFORM'] == 'Shopify') 
                               & (market_data['GMV_CATEGORY'] != 'a) < $1M')
                               & (market_data['POLAR ARR ($)'] > 0)]
    fig2 = px.treemap(shopify_data, 
                             path=['GMV_CATEGORY', 'COUNTRY'], 
                             values='POLAR ARR ($)', 
                             title="Potential New ARR by GMV Category & Country",
                             labels={'POLAR ARR ($)': 'Potential ARR ($)'},
                             color='POLAR ARR ($)',
                             color_continuous_scale='blues')
    fig2.update_layout(height=600)
    st.plotly_chart(fig2, use_container_width=True)
    Potential_ARR = shopify_data['POLAR ARR ($)'].sum()
    US_Potential_ARR = shopify_data[shopify_data['COUNTRY'] == 'United States']['POLAR ARR ($)'].sum()
    US_Potential_ARR_Percent = (US_Potential_ARR / Potential_ARR) * 100
    st.markdown(f"""
                - The total potential ARR from scaling to the Total Addressable Market (TAM) is **${Potential_ARR:,.0f}**.
                - The treemap shows how potential new ARR ($) from scaling to the TAM is distributed across different countries,
                helping prioritize outbound expansion. Clearly, the United States presents the most valuable opportunity
                making up **{US_Potential_ARR_Percent:.0f}%** of the total opportunity.
                """)
    st.divider()

    # Prioritize outbound as a growth lever
    st.subheader("Acquisition Mix Evaluation: Outbound as a Growth Lever")
    funnel_data = pd.DataFrame({
        'Stage': [
            'Contacts Touched', 
            'Companies Touched', 
            'ICP Companies Touched', 
            'ICP Companies Clicked', 
            'ICP Companies Replied', 
            'ICP Positive Replies'
        ],
        'Count': [
            outbound_data['NB_CONTACTS_TOUCHED'].sum(),
            outbound_data['NB_COMPANIES_TOUCHED'].sum(),
            outbound_data['NB_COMPANIES_TOUCHED_ICP'].sum(),
            outbound_data['NB_COMPANIES_CLICKED_ICP'].sum(),
            outbound_data['NB_COMPANIES_REPLIED_ICP'].sum(),
            outbound_data['NB_COMPANIES_REPLIED_POSITIVE_ICP'].sum()
        ]
    })
    fig = px.funnel(funnel_data, x='Count', y='Stage', title="Outbound Campaign Funnel")
    st.plotly_chart(fig)

    total_new_arr_outbound = outbound_data['NEW_ARR_FROM_OB_ALL_TIME'].sum()
    outbound_contribution_percentage = (total_new_arr_outbound / Potential_ARR) * 100
    st.write(f"Total New ARR from Outbound Campaigns: ${total_new_arr_outbound:,.0f}")
    st.write(f"Total Addressable Market (TAM): ${Potential_ARR:,.0f}")
    st.write(f"Outbound Campaigns contribute {outbound_contribution_percentage:.2f}% to the Total Addressable Market (TAM)")

    # Calculate conversion rates for ICP
    outbound_data['ICP_click_through_rate'] = outbound_data['NB_COMPANIES_CLICKED_ICP'] / outbound_data['NB_COMPANIES_TOUCHED_ICP']
    outbound_data['ICP_reply_rate'] = outbound_data['NB_COMPANIES_REPLIED_ICP'] / outbound_data['NB_COMPANIES_TOUCHED_ICP']
    outbound_data['ICP_positive_reply_rate'] = outbound_data['NB_COMPANIES_REPLIED_POSITIVE_ICP'] / outbound_data['NB_COMPANIES_TOUCHED_ICP']
    avg_icp_click_through_rate = outbound_data['ICP_click_through_rate'].mean()
    avg_icp_reply_rate = outbound_data['ICP_reply_rate'].mean()
    avg_icp_positive_reply_rate = outbound_data['ICP_positive_reply_rate'].mean()

    limitations = []

    if avg_icp_click_through_rate < 0.2:
        limitations.append(f"    \n- Low click-through rate for ICP contacts. ({avg_icp_click_through_rate:.2f}%)")

    if avg_icp_reply_rate < 0.1: 
        limitations.append(f"    \n- Low reply rate from ICP contacts. ({avg_icp_reply_rate:.2f}%)")

    if avg_icp_positive_reply_rate < 0.05:
        limitations.append(f"    \n- Low positive reply rate from ICP contacts. ({avg_icp_positive_reply_rate:.2f}%)")

    # Check if the number of ICP companies contacted is low (potential data limitation)
    total_icp_companies_contacted = outbound_data['NB_COMPANIES_TOUCHED_ICP'].sum()
    total_companies_contacted = outbound_data['NB_COMPANIES_TOUCHED'].sum()
    total_icp_companies_contacted_percentage = total_icp_companies_contacted/total_companies_contacted

    if total_icp_companies_contacted < 1000: 
        limitations.append("Limited ICP data – not enough ICP companies are being contacted.")

    # Final Recommendation based on limitations
    if limitations:
        limitations_message = f"\nOutbound is facing the following limitations:\n" + "\n".join(limitations)
    else:
        limitations_message = "Outbound is performing well with respect to ICP conversion rates."
    st.markdown(f""" {limitations_message}""")
    with st.container(border = True):
        # Your content
        st.markdown(f"""
        #### **_Key Insights for a Scalable, Synergized Acquisition Mix_**
        
        Given that **Outbound** makes up **{outbound_contribution_percentage:.2f}%** of the total TAM, that **ICP customers** represent only **{total_icp_companies_contacted_percentage:.2f}%** of companies reached in all campaigns, and assuming that **Inbound** and **Ads** channels perform similarly, a well-balanced & scalable acquisition mix should consider:

        🚀 **Outbound Scaling Considerations:** Common constraints like **ICP quality** and **sales team capacity** should be evaluated before scaling further. Improving **targeting efficiency** (e.g., automated outreach) and expanding the **ICP pool** may be necessary.
        
        🔄 **Outbound Optimization:** Outbound works well for **high-touch engagement** but is limited by **ICP data & conversion rates**. Scaling effectively requires **automation & personalization**.
        
        🌱 **Inbound for Long-Term Growth:** Inbound is **less resource-intensive** and builds a **sustainable sales pipeline**. Feeding **high-quality leads** into the sales funnel enhances outbound efficiency.
        
        🎯 **Paid Ads for Immediate Impact:** Paid ads provide **precision & quick wins**, but **CAC must not outweigh CLV**. Combining ads with outbound **retargeting** helps optimize ad spend on high-win-rate customers.
        """)
    st.divider()

    # Recommended next steps
    st.subheader("Recommended Next Steps")
    st.markdown("""
1. **_Improve ICP Data Quality_**: Enhance ICP data quality by leveraging data enrichment tools to gather more insights about ICP companies. The US remains a key market, so the focus should be on experimenting with expansion and segmentation of the US ICP data based on more granular criteria like company size, decision-maker titles, and specific pain points.
2. **_Automated Tailored Messaging_:** Automate tailored, personalized messaging for ICP contacts to improve click-through and reply rates. This could involve A/B testing different messaging strategies to identify the most effective approach.
3. **_Improve Call to Action_:** Experiment with different types of CTAs (e.g., demo request, case study, free trial) and assess which ones resonate best with the target audience. Bonus: Taking the buyer journey into account, align the CTA with the stage of the funnel: early-stage leads may benefit from educational content, while later-stage leads may prefer speaking to Inside Sales.
""")