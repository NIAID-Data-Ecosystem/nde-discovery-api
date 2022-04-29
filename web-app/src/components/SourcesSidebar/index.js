import { useCallback } from "react";
import { scroller } from "react-scroll";
import { useState, useEffect } from 'react'
import { setName } from "../../utils/setFunctions";

const SourcesSidebar = ({ sourceData }) => {
    //This useCallback function taken a string argument of date and converts it into formatted date, e.g Day Month Date Year(Monday 1 Jan 1999)
    const date = useCallback((data) => {
        let dateString;
        dateString = new Date(data);
        return dateString.toDateString();
    }, []);


    const sourceNames = Object.keys(sourceData.src)


    function handleNav(version) {
        scroller.scrollTo(`version${version}`, {
            duration: 1000,
            delay: 0,
            smooth: "easeInOutQuart",
            offset: -5,
        })
    }; // This function helps in navigation between versions
    return (
        sourceNames
            .map((name, index) => {
                return (
                    <li key={index} className={"border-none m-2 ml-4 pb-4"}>
                        <a
                            className={"flex flex-col items-left h-14  cursor-pointer "}
                            onClick={() => handleNav(name)} // This onClick function will act as navigation between different section we have
                        >
                            <div className="text-gray-900 text-xl font-bold  ">
                                {setName(name)} <br />
                            </div>
                            <div className="text-sm font-medium text-gray-500">
                                Released {date(sourceData.src[name].version)}
                            </div>
                        </a>
                    </li>
                );
            }))
};

export default SourcesSidebar;
