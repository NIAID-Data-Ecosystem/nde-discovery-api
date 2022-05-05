import SourcesHeader from "../SourcesHeader";
import SourcesMain from "../SourcesMain";
import { useState, useEffect } from 'react'
import SourcesSidebar from "../SourcesSidebar";
import './sourcessidebar.css'

const SourcesUI = () => {

    const [sourceData, setSourceData] = useState({})
    const [ready, setReady] = useState(false)

    const sourceURL = 'https://api.data.niaid.nih.gov/v1/metadata/'

    const getSourcesData = async () => {
        const response = await fetch(sourceURL)
        const jsonData = await response.json()
        setSourceData(jsonData)
    }


    useEffect(() => {
        getSourcesData()
    }, [])

    useEffect(() => {
        if (Object.keys(sourceData).length > 0) setReady(true)
    }, [sourceData])

    return (
        <>
            {ready &&
                <>
                    <SourcesHeader />
                    <div className="min-h-screen flex flex-row">
                        <div className="flex flex-col bg-gray-100 sidebar-container  text-white w-6/12 mobile:hidden">
                            <ul className="flex flex-col py-4 sticky top-0  divide-y divide-y divide-gray-400">
                                <SourcesSidebar sourceData={sourceData} />
                            </ul>
                        </div>
                        <div className="main-container">
                            <SourcesMain sourceData={sourceData} />
                        </div>
                    </div>
                </>
            }
        </>
    );
};

export default SourcesUI;
