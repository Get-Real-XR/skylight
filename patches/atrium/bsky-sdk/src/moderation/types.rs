use super::decision::{DecisionContext, Priority};
use super::error::Error;
use atrium_api::agent::bluesky::BSKY_LABELER_DID;
use atrium_api::app::bsky::actor::defs::{
    MutedWord, ProfileView, ProfileViewBasic, ProfileViewDetailed, ViewerState,
};
use atrium_api::app::bsky::graph::defs::{ListView, ListViewBasic};
use atrium_api::com::atproto::label::defs::{Label, LabelValueDefinitionStrings};
use atrium_api::types::string::Did;
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::HashMap, str::FromStr};

// behaviors

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BehaviorValue {
    Blur,
    Alert,
    Inform,
}

/// Moderation behaviors for different contexts.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModerationBehavior {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_list: Option<ProfileListBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_view: Option<ProfileViewBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar: Option<AvatarBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<BannerBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<DisplayNameBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_list: Option<ContentListBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_view: Option<ContentViewBehavior>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_media: Option<ContentMediaBehavior>,
}

impl ModerationBehavior {
    pub(crate) const BLOCK_BEHAVIOR: Self = Self {
        profile_list: Some(ProfileListBehavior::Blur),
        profile_view: Some(ProfileViewBehavior::Alert),
        avatar: Some(AvatarBehavior::Blur),
        banner: Some(BannerBehavior::Blur),
        display_name: None,
        content_list: Some(ContentListBehavior::Blur),
        content_view: Some(ContentViewBehavior::Blur),
        content_media: None,
    };
    pub(crate) const MUTE_BEHAVIOR: Self = Self {
        profile_list: Some(ProfileListBehavior::Inform),
        profile_view: Some(ProfileViewBehavior::Alert),
        avatar: None,
        banner: None,
        display_name: None,
        content_list: Some(ContentListBehavior::Blur),
        content_view: Some(ContentViewBehavior::Inform),
        content_media: None,
    };
    pub(crate) const MUTEWORD_BEHAVIOR: Self = Self {
        profile_list: None,
        profile_view: None,
        avatar: None,
        banner: None,
        display_name: None,
        content_list: Some(ContentListBehavior::Blur),
        content_view: Some(ContentViewBehavior::Blur),
        content_media: None,
    };
    pub(crate) const HIDE_BEHAVIOR: Self = Self {
        profile_list: None,
        profile_view: None,
        avatar: None,
        banner: None,
        display_name: None,
        content_list: Some(ContentListBehavior::Blur),
        content_view: Some(ContentViewBehavior::Blur),
        content_media: None,
    };
    pub(crate) fn behavior_for(&self, context: DecisionContext) -> Option<BehaviorValue> {
        match context {
            DecisionContext::ProfileList => self.profile_list.clone().map(Into::into),
            DecisionContext::ProfileView => self.profile_view.clone().map(Into::into),
            DecisionContext::Avatar => self.avatar.clone().map(Into::into),
            DecisionContext::Banner => self.banner.clone().map(Into::into),
            DecisionContext::DisplayName => self.display_name.clone().map(Into::into),
            DecisionContext::ContentList => self.content_list.clone().map(Into::into),
            DecisionContext::ContentView => self.content_view.clone().map(Into::into),
            DecisionContext::ContentMedia => self.content_media.clone().map(Into::into),
        }
    }
}

/// Moderation behaviors for the profile list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfileListBehavior {
    Blur,
    Alert,
    Inform,
}

impl From<ProfileListBehavior> for BehaviorValue {
    fn from(b: ProfileListBehavior) -> Self {
        match b {
            ProfileListBehavior::Blur => Self::Blur,
            ProfileListBehavior::Alert => Self::Alert,
            ProfileListBehavior::Inform => Self::Inform,
        }
    }
}

impl TryFrom<BehaviorValue> for ProfileListBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            BehaviorValue::Alert => Ok(Self::Alert),
            BehaviorValue::Inform => Ok(Self::Inform),
        }
    }
}

/// Moderation behaviors for the profile view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfileViewBehavior {
    Blur,
    Alert,
    Inform,
}

impl From<ProfileViewBehavior> for BehaviorValue {
    fn from(b: ProfileViewBehavior) -> Self {
        match b {
            ProfileViewBehavior::Blur => Self::Blur,
            ProfileViewBehavior::Alert => Self::Alert,
            ProfileViewBehavior::Inform => Self::Inform,
        }
    }
}

impl TryFrom<BehaviorValue> for ProfileViewBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            BehaviorValue::Alert => Ok(Self::Alert),
            BehaviorValue::Inform => Ok(Self::Inform),
        }
    }
}

/// Moderation behaviors for the user's avatar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AvatarBehavior {
    Blur,
    Alert,
}

impl From<AvatarBehavior> for BehaviorValue {
    fn from(b: AvatarBehavior) -> Self {
        match b {
            AvatarBehavior::Blur => Self::Blur,
            AvatarBehavior::Alert => Self::Alert,
        }
    }
}

impl TryFrom<BehaviorValue> for AvatarBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            BehaviorValue::Alert => Ok(Self::Alert),
            BehaviorValue::Inform => Err(Error::BehaviorValue),
        }
    }
}

/// Moderation behaviors for the user's banner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BannerBehavior {
    Blur,
}

impl From<BannerBehavior> for BehaviorValue {
    fn from(b: BannerBehavior) -> Self {
        match b {
            BannerBehavior::Blur => Self::Blur,
        }
    }
}

impl TryFrom<BehaviorValue> for BannerBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            _ => Err(Error::BehaviorValue),
        }
    }
}

/// Moderation behaviors for the user's display name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DisplayNameBehavior {
    Blur,
}

impl From<DisplayNameBehavior> for BehaviorValue {
    fn from(b: DisplayNameBehavior) -> Self {
        match b {
            DisplayNameBehavior::Blur => Self::Blur,
        }
    }
}

impl TryFrom<BehaviorValue> for DisplayNameBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            _ => Err(Error::BehaviorValue),
        }
    }
}

/// Moderation behaviors for the content list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentListBehavior {
    Blur,
    Alert,
    Inform,
}

impl From<ContentListBehavior> for BehaviorValue {
    fn from(b: ContentListBehavior) -> Self {
        match b {
            ContentListBehavior::Blur => Self::Blur,
            ContentListBehavior::Alert => Self::Alert,
            ContentListBehavior::Inform => Self::Inform,
        }
    }
}

impl TryFrom<BehaviorValue> for ContentListBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            BehaviorValue::Alert => Ok(Self::Alert),
            BehaviorValue::Inform => Ok(Self::Inform),
        }
    }
}

/// Moderation behaviors for the content view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentViewBehavior {
    Blur,
    Alert,
    Inform,
}

impl From<ContentViewBehavior> for BehaviorValue {
    fn from(b: ContentViewBehavior) -> Self {
        match b {
            ContentViewBehavior::Blur => Self::Blur,
            ContentViewBehavior::Alert => Self::Alert,
            ContentViewBehavior::Inform => Self::Inform,
        }
    }
}

impl TryFrom<BehaviorValue> for ContentViewBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            BehaviorValue::Alert => Ok(Self::Alert),
            BehaviorValue::Inform => Ok(Self::Inform),
        }
    }
}

/// Moderation behaviors for the content media.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentMediaBehavior {
    Blur,
}

impl From<ContentMediaBehavior> for BehaviorValue {
    fn from(b: ContentMediaBehavior) -> Self {
        match b {
            ContentMediaBehavior::Blur => Self::Blur,
        }
    }
}

impl TryFrom<BehaviorValue> for ContentMediaBehavior {
    type Error = Error;

    fn try_from(b: BehaviorValue) -> Result<Self, Self::Error> {
        match b {
            BehaviorValue::Blur => Ok(Self::Blur),
            _ => Err(Error::BehaviorValue),
        }
    }
}

// labels

/// The target of a label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelTarget {
    Account,
    Profile,
    Content,
}

/// The preference for a label.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LabelPreference {
    Ignore,
    Warn,
    Hide,
}

impl AsRef<str> for LabelPreference {
    fn as_ref(&self) -> &str {
        match self {
            Self::Ignore => "ignore",
            Self::Warn => "warn",
            Self::Hide => "hide",
        }
    }
}

impl FromStr for LabelPreference {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ignore" => Ok(Self::Ignore),
            "warn" => Ok(Self::Warn),
            "hide" => Ok(Self::Hide),
            _ => Err(Error::LabelPreference),
        }
    }
}

/// A flag for a label value definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LabelValueDefinitionFlag {
    NoOverride,
    Adult,
    Unauthed,
    NoSelf,
}

/// The blurs for a label value definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LabelValueDefinitionBlurs {
    Content,
    Media,
    None,
}

impl AsRef<str> for LabelValueDefinitionBlurs {
    fn as_ref(&self) -> &str {
        match self {
            Self::Content => "content",
            Self::Media => "media",
            Self::None => "none",
        }
    }
}

impl FromStr for LabelValueDefinitionBlurs {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "content" => Ok(Self::Content),
            "media" => Ok(Self::Media),
            "none" => Ok(Self::None),
            _ => Err(Error::LabelValueDefinitionBlurs),
        }
    }
}

/// The severity for a label value definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LabelValueDefinitionSeverity {
    Inform,
    Alert,
    None,
}

impl AsRef<str> for LabelValueDefinitionSeverity {
    fn as_ref(&self) -> &str {
        match self {
            Self::Inform => "inform",
            Self::Alert => "alert",
            Self::None => "none",
        }
    }
}

impl FromStr for LabelValueDefinitionSeverity {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "inform" => Ok(Self::Inform),
            "alert" => Ok(Self::Alert),
            "none" => Ok(Self::None),
            _ => Err(Error::LabelValueDefinitionSeverity),
        }
    }
}

/// A label value definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InterpretedLabelValueDefinition {
    // from com.atproto.label/defs#labelValueDefinition, with type narrowing
    pub adult_only: bool,
    pub blurs: LabelValueDefinitionBlurs,
    pub default_setting: LabelPreference,
    pub identifier: String,
    pub locales: Vec<LabelValueDefinitionStrings>,
    pub severity: LabelValueDefinitionSeverity,
    // others
    #[serde(skip_serializing_if = "Option::is_none")]
    pub defined_by: Option<Did>,
    pub configurable: bool,
    pub flags: Vec<LabelValueDefinitionFlag>,
    pub behaviors: InterpretedLabelValueDefinitionBehaviors,
}

/// The behaviors for a label value definition.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InterpretedLabelValueDefinitionBehaviors {
    pub account: ModerationBehavior,
    pub profile: ModerationBehavior,
    pub content: ModerationBehavior,
}

impl InterpretedLabelValueDefinitionBehaviors {
    pub(crate) fn behavior_for(&self, target: LabelTarget) -> ModerationBehavior {
        match target {
            LabelTarget::Account => self.account.clone(),
            LabelTarget::Profile => self.profile.clone(),
            LabelTarget::Content => self.content.clone(),
        }
    }
}

// subjects

/// A subject profile.
#[derive(Debug)]
pub enum SubjectProfile {
    ProfileViewBasic(Box<ProfileViewBasic>),
    ProfileView(Box<ProfileView>),
    ProfileViewDetailed(Box<ProfileViewDetailed>),
}

impl SubjectProfile {
    pub(crate) fn did(&self) -> &Did {
        match self {
            Self::ProfileViewBasic(p) => &p.did,
            Self::ProfileView(p) => &p.did,
            Self::ProfileViewDetailed(p) => &p.did,
        }
    }
    pub(crate) fn labels(&self) -> &Option<Vec<Label>> {
        match self {
            Self::ProfileViewBasic(p) => &p.labels,
            Self::ProfileView(p) => &p.labels,
            Self::ProfileViewDetailed(p) => &p.labels,
        }
    }
    pub(crate) fn viewer(&self) -> &Option<ViewerState> {
        match self {
            Self::ProfileViewBasic(p) => &p.viewer,
            Self::ProfileView(p) => &p.viewer,
            Self::ProfileViewDetailed(p) => &p.viewer,
        }
    }
}

impl From<ProfileViewBasic> for SubjectProfile {
    fn from(p: ProfileViewBasic) -> Self {
        Self::ProfileViewBasic(Box::new(p))
    }
}

impl From<ProfileView> for SubjectProfile {
    fn from(p: ProfileView) -> Self {
        Self::ProfileView(Box::new(p))
    }
}

impl From<ProfileViewDetailed> for SubjectProfile {
    fn from(p: ProfileViewDetailed) -> Self {
        Self::ProfileViewDetailed(Box::new(p))
    }
}

/// A subject post.
pub type SubjectPost = atrium_api::app::bsky::feed::defs::PostView;

/// A subject notification.
pub type SubjectNotification =
    atrium_api::app::bsky::notification::list_notifications::Notification;

/// A subject feed generator.
pub type SubjectFeedGenerator = atrium_api::app::bsky::feed::defs::GeneratorView;

/// A subject user list.
#[derive(Debug)]
pub enum SubjectUserList {
    ListView(Box<ListView>),
    ListViewBasic(Box<ListViewBasic>),
}

impl From<ListView> for SubjectUserList {
    fn from(list_view: ListView) -> Self {
        Self::ListView(Box::new(list_view))
    }
}

impl From<ListViewBasic> for SubjectUserList {
    fn from(list_view_basic: ListViewBasic) -> Self {
        Self::ListViewBasic(Box::new(list_view_basic))
    }
}

/// A cause for moderation decisions.
#[derive(Debug, Clone)]
pub enum ModerationCause {
    Blocking(Box<ModerationCauseOther>),
    BlockedBy(Box<ModerationCauseOther>),
    // BlockOther(Box<ModerationCauseOther>),
    Label(Box<ModerationCauseLabel>),
    Muted(Box<ModerationCauseOther>),
    MuteWord(Box<ModerationCauseOther>),
    Hidden(Box<ModerationCauseOther>),
}

impl ModerationCause {
    pub fn priority(&self) -> u8 {
        match self {
            Self::Blocking(_) => *Priority::Priority3.as_ref(),
            Self::BlockedBy(_) => *Priority::Priority4.as_ref(),
            Self::Label(label) => *label.priority.as_ref(),
            Self::Muted(_) => *Priority::Priority6.as_ref(),
            Self::MuteWord(_) => *Priority::Priority6.as_ref(),
            Self::Hidden(_) => *Priority::Priority6.as_ref(),
        }
    }
    pub fn downgrade(&mut self) {
        match self {
            Self::Blocking(blocking) => blocking.downgraded = true,
            Self::BlockedBy(blocked_by) => blocked_by.downgraded = true,
            Self::Label(label) => label.downgraded = true,
            Self::Muted(muted) => muted.downgraded = true,
            Self::MuteWord(mute_word) => mute_word.downgraded = true,
            Self::Hidden(hidden) => hidden.downgraded = true,
        }
    }
}

/// The source of a moderation cause.
#[derive(Debug, Clone)]
pub enum ModerationCauseSource {
    User,
    List(Box<ListViewBasic>),
    Labeler(Did),
}

/// A label moderation cause.
#[derive(Debug, Clone)]
pub struct ModerationCauseLabel {
    pub source: ModerationCauseSource,
    pub label: Label,
    pub label_def: InterpretedLabelValueDefinition,
    pub target: LabelTarget,
    pub setting: LabelPreference,
    pub behavior: ModerationBehavior,
    pub no_override: bool,
    pub(crate) priority: Priority,
    pub downgraded: bool,
}

/// An other moderation cause.
#[derive(Debug, Clone)]
pub struct ModerationCauseOther {
    pub source: ModerationCauseSource,
    pub downgraded: bool,
}

// moderation preferences

/// The labeler preferences for moderation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModerationPrefsLabeler {
    pub did: Did,
    pub labels: HashMap<String, LabelPreference>,
    #[serde(skip_serializing, skip_deserializing)]
    pub is_default_labeler: bool,
}

impl Default for ModerationPrefsLabeler {
    fn default() -> Self {
        Self {
            did: BSKY_LABELER_DID.parse().expect("invalid did"),
            labels: HashMap::default(),
            is_default_labeler: true,
        }
    }
}

/// The moderation preferences.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModerationPrefs {
    pub adult_content_enabled: bool,
    pub labels: HashMap<String, LabelPreference>,
    #[serde(deserialize_with = "deserialize_labelers")]
    pub labelers: Vec<ModerationPrefsLabeler>,
    pub muted_words: Vec<MutedWord>,
    pub hidden_posts: Vec<String>,
}

fn deserialize_labelers<'de, D>(deserializer: D) -> Result<Vec<ModerationPrefsLabeler>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut labelers: Vec<ModerationPrefsLabeler> = Deserialize::deserialize(deserializer)?;
    for labeler in labelers.iter_mut() {
        if labeler.did.as_str() == BSKY_LABELER_DID {
            labeler.is_default_labeler = true;
        }
    }
    Ok(labelers)
}

impl Default for ModerationPrefs {
    fn default() -> Self {
        Self {
            adult_content_enabled: false,
            labels: HashMap::from_iter([
                (String::from("porn"), LabelPreference::Hide),
                (String::from("sexual"), LabelPreference::Warn),
                (String::from("nudity"), LabelPreference::Ignore),
                (String::from("graphic-media"), LabelPreference::Warn),
            ]),
            labelers: Vec::default(),
            muted_words: Vec::default(),
            hidden_posts: Vec::default(),
        }
    }
}
