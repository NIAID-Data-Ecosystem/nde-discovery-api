import React from 'react';
import { useState, useEffect } from 'react'
import { Navigation } from "nde-design-system";
import Main from '../Main';
import Sidebar from '../Sidebar';

const UI = () => {
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
                    <Navigation />
                    <div className="min-h-screen flex flex-row">
                        <div className="flex-col bg-gray-100  text-white w-6/12 hidden md:block">
                            <ul className="flex flex-col py-4 sticky top-0 divide-gray-400">
                                <Sidebar />
                            </ul>
                        </div>
                        <div className=" text-center md:text-left">
                            <Main sourceData={sourceData} />
                        </div>
                    </div>
                </>
            }
        </>
    );
};

export default UI;
