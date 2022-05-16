import { useCallback } from "react";
import { scroller } from "react-scroll";


const Sidebar = () => {

    function handleNav(version) {
        scroller.scrollTo(`version${version}`, {
            duration: 1000,
            delay: 0,
            smooth: "easeInOutQuart",
            offset: -5,
        })
    };

    return (
        <>
            <li className={"border-none m-2 ml-4 pb-4"}>
                <a
                    className={"version1.0.0 flex flex-col items-left h-14  cursor-pointer "}
                    onClick={() => handleNav('1.0.0')}
                >
                    <div className="text-gray-900 text-xl font-bold  ">
                        Version 1.0.0 <br />
                    </div>
                    <div className="text-sm font-medium text-gray-500">
                        Release Date April 21 2022
                    </div>
                </a>
            </li>
        </>
    )
}
export default Sidebar;
