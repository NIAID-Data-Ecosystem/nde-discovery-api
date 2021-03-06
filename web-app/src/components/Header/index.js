import './header.css'
import logo from './logo.svg'
const Header = () => {
    return (
        <nav className="release-header flex items-center justify-between flex-wrap bg-blue-600  p-6">
            <div className="flex items-center flex-no-shrink text-white mr-6 w-full justify-start">
                <a href="https://data.niaid.nih.gov/">
                    <img className='logo' src={logo} />
                </a>
                <span className="header-title font-semibold text-xl tracking-tight">
                    NIAID Data Ecosystem Discovery API Release Notes
                </span>
            </div>
        </nav>
    );
};

export default Header;
