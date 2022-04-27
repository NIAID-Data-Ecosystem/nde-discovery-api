import { useCallback } from "react";
import { scroller } from "react-scroll";


const Sidebar = ({ versions, loading }) => {
    //This useCallback function taken a string argument of date and converts it into formatted date, e.g Day Month Date Year(Monday 1 Jan 1999)
    const date = useCallback((data) => {
        let dateString;
        dateString = new Date(data);
        return dateString.toDateString();
    }, []);

    const ready = versions && !loading; // This variable checks if data is present is versions and not in loading

    function handleNav(version) {
        scroller.scrollTo(`version${version}`, {
            duration: 1000,
            delay: 0,
            smooth: "easeInOutQuart",
            offset: -80,
        })
    }; // This function helps in navigation between versions

    return (
        <>
            {loading && <h1 className="text-gray-900">Loading...</h1>}
            {ready &&
                versions
                    .map((data, index) => {
                        return (

                            <li key={index} className={"m-2 ml-4 pb-4"}>
                                <a
                                    className={"flex flex-col items-left h-14  cursor-pointer "}
                                    onClick={() => handleNav(data.version)} // This onClick function will act as navigation between different section we have
                                >
                                    <div className="text-gray-900 text-2xl font-bold  ">
                                        {data.version} <br />
                                    </div>
                                    <div className="text-sm font-medium text-gray-500">
                                        {date(data.date)}
                                    </div>
                                </a>
                            </li>
                        );
                    })
                    .reverse()}
        </>
    );
};

export default Sidebar;
