import React from 'react';
import NavbarLayout from '@theme/Navbar/Layout';
import {useThemeConfig, useColorMode} from '@docusaurus/theme-common';
import {useNavbarMobileSidebar} from '@docusaurus/theme-common/internal';
import NavbarItem from '@theme/NavbarItem';
import NavbarColorModeToggle from '@theme/Navbar/ColorModeToggle';
import SearchBar from '@theme/SearchBar';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Link from '@docusaurus/Link';
import {FaBars} from 'react-icons/fa';
import GitHubStar from '../../components/GitHubStar';
import VersionSelector from '../../components/VersionSelector';
import type {Props as NavbarItemConfig} from '@theme/NavbarItem';
import styles from './styles.module.css';

export default function Navbar(): React.ReactElement {
  const mobileSidebar = useNavbarMobileSidebar();
  const {colorMode} = useColorMode();
  const {navbar: {items, logo, title}} = useThemeConfig();
  const logoSrc = colorMode === 'dark' ? 'img/logo-dark.svg' : 'img/logo.svg';
  const iconSrc = 'img/icon.svg';
  const leftItems = items.filter(item => item.position === 'left');
  const rightItems = items.filter(item => item.position === 'right');

  return (
    <NavbarLayout>
      <div className={styles.navbarContainer}>
        <button
          className={`${styles.navbarMobileMenuButton} ${mobileSidebar.shown ? styles.navbarMobileMenuButtonHidden : ''}`}
          onClick={mobileSidebar.toggle}
          aria-label="Toggle navigation bar"
          aria-expanded={mobileSidebar.shown}
        >
          <FaBars className={styles.navbarMobileMenuIcon} />
        </button>

        <div className={styles.navbarBrand}>
          <Link to={logo?.href || '/'} className={styles.navbarLogoLink}>
            <img
              className={styles.navbarLogo}
              src={useBaseUrl(logoSrc)}
              alt={logo?.alt || title}
            />
            <img
              className={styles.navbarIcon}
              src={useBaseUrl(iconSrc)}
              alt={logo?.alt || title}
            />
          </Link>
        </div>

        <div className={styles.navbarItemsLeft}>
          {leftItems.map((item, i) => <NavbarItem {...(item as NavbarItemConfig)} key={i} />)}
        </div>

        <div className={styles.navbarItemsRight}>
          {rightItems.map((item, i) => <NavbarItem {...(item as NavbarItemConfig)} key={i} />)}
          <div className={styles.versionSelectorWrapper}>
            <VersionSelector />
          </div>
          <div className={styles.githubStarWrapper}>
            <GitHubStar />
          </div>
          <NavbarColorModeToggle className={styles.colorModeToggle} />
          <div className={styles.navbarSearch}>
            <SearchBar />
          </div>
        </div>
      </div>
    </NavbarLayout>
  );
}
